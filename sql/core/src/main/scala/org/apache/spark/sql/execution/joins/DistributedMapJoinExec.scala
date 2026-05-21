/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.joins

import java.util

import scala.annotation.tailrec
import scala.concurrent.ExecutionContextExecutorService

import io.netty.buffer.Unpooled

import org.apache.spark.{SparkEnv, SparkException, TaskContext}
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.shard.ShardSetRef
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow, JoinedRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{DistMapJoinStrategy, JoinHint}
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{BufferedShardRowMap, SparkPlan}
import org.apache.spark.sql.execution.adaptive.ShardQueryStageExec
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShardExchangeExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.sketch.BloomFilter

/**
 * Physical operator for Distributed MapJoin.
 *
 * This strategy avoids full shuffle by building a distributed hash table service for the build
 * side (medium-sized table), and the probe side performs batched RPC lookups to complete the
 * join.
 *
 * Currently only supports:
 *   - Equi-join
 *   - BuildRight (right table as build side)
 *   - Explicit SQL hint: /*+distmapjoin(t(shard_count=5,replica_count=2))*/
 */
case class DistributedMapJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    hint: JoinHint,
    isNullAwareAntiJoin: Boolean = false)
    extends HashJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val (numShards, replicaCount): (Int, Int) = {
    val strategy =
      (if (buildSide == BuildLeft) hint.leftHint else hint.rightHint).flatMap(_.strategy)
    strategy match {
      case Some(DistMapJoinStrategy(ns, rc)) => (ns.getOrElse(5), rc.getOrElse(1))
      case _ => (5, 1)
    }
  }

  override def supportCodegen: Boolean = false

  override def supportsColumnar: Boolean = false

  override def needCopyResult: Boolean = false

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      Seq(ShardDistribution(buildBoundKeys, numShards, replicaCount), UnspecifiedDistribution)
    case BuildRight =>
      Seq(UnspecifiedDistribution, ShardDistribution(buildBoundKeys, numShards, replicaCount))
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def inputRDDs(): Seq[RDD[InternalRow]] =
    throw QueryExecutionErrors.executeCodePathUnsupportedError("DistributedMapJoin")

  override protected def prepareRelation(ctx: CodegenContext): HashedRelationInfo =
    throw QueryExecutionErrors.executeCodePathUnsupportedError("DistributedMapJoin")

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan,
      newRight: SparkPlan): SparkPlan = copy(left = newLeft, right = newRight)

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val setRef = resolveShardSetRef(buildPlan)
    streamedPlan.execute().mapPartitionsInternal { streamedIter =>
      join(streamedIter, setRef.setId, numOutputRows)
    }
  }

  @tailrec
  private def resolveShardSetRef(plan: SparkPlan): ShardSetRef = plan match {
    case s: ShardExchangeExec => s.buildShardSet()
    case s: ShardQueryStageExec => s.shardSetRef
    case r: ReusedExchangeExec => resolveShardSetRef(r.child)
    case other =>
      throw new IllegalStateException(s"Unexpected build plan for DistributedMapJoin: $other")
  }

  private def streamedBloomFilter(setId: Long): BloomFilter = {
    SparkEnv.get.shardManager.fetchBloomFilter[BloomFilter](setId)(bfInput =>
      BloomFilter.readFrom(bfInput))
  }

  private def join(
      streamedIter: Iterator[InternalRow],
      setId: Long,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {

    val joinedIter: Iterator[InternalRow] = joinType match {
      case _: InnerLike =>
        innerJoin(streamedIter, setId)
      case LeftOuter | RightOuter =>
        outerJoin(streamedIter, setId)
      case LeftSemi =>
        semiJoin(streamedIter, setId)
      case LeftAnti =>
        antiJoin(streamedIter, setId)
      case _: ExistenceJoin =>
        existenceJoin(streamedIter, setId)
      case x =>
        throw new IllegalArgumentException(
          s"DistributedMapJoin should not take $x as the JoinType")
    }

    val resultProj = createResultProjection()
    joinedIter.map { row =>
      numOutputRows.add(1)
      resultProj(row)
    }
  }

  private def innerJoin(streamedIter: Iterator[InternalRow], setId: Long): Iterator[InternalRow] =
    new LookupJoinIterator(streamedIter, setId) {
      override protected def onMatches(batch: PBatch, buffer: BBuffer): GenericRowIterator =
        new GenericRowIterator(batch, buffer) {

          override protected def whichUr: UnsafeRow = buildUr

          override protected def onJoin(sr: UnsafeRow, br: UnsafeRow): Boolean =
            boundCondition(joinedRow.withLeft(sr).withRight(br))

          override def rowToEmit: InternalRow = joinedRow
        }
    }

  private def outerJoin(streamedIter: Iterator[InternalRow], setId: Long): Iterator[InternalRow] =
    new LookupJoinIterator(streamedIter, setId) {

      private val nullBuildRow = new GenericInternalRow(buildOutput.length)

      override protected def onNullKey(streamedRow: InternalRow): Unit =
        emitNullSupplied(streamedRow, nullBuildRow)

      override protected def onBloomNegative(streamedRow: InternalRow): Unit =
        emitNullSupplied(streamedRow, nullBuildRow)

      override protected def onMatches(batch: PBatch, buffer: BBuffer): GenericRowIterator =
        new GenericRowIterator(batch, buffer) {

          private var found: Boolean = _

          override protected def whichUr: UnsafeRow = buildUr

          override protected def onStreamedRow(sr: UnsafeRow): Unit = { found = false }

          override protected def onJoin(sr: UnsafeRow, br: UnsafeRow): Boolean = {
            if (boundCondition(joinedRow.withLeft(sr).withRight(br))) {
              found = true
              true
            } else false
          }

          override protected def onEndOfKey(sr: UnsafeRow): Boolean = {
            if (!found) {
              joinedRow.withLeft(sr).withRight(nullBuildRow)
              true
            } else false
          }

          protected def rowToEmit: InternalRow = joinedRow
        }
    }

  private def semiJoin(streamedIter: Iterator[InternalRow], setId: Long): Iterator[InternalRow] =
    new LookupJoinIterator(streamedIter, setId) {

      override protected def onMatches(batch: PBatch, buffer: BBuffer): GenericRowIterator =
        new GenericRowIterator(batch, buffer) {

          private var found: Boolean = _

          private var foundVal: UnsafeRow = _

          override protected def whichUr: UnsafeRow = buildUr

          override protected def onStreamedRow(sr: UnsafeRow): Unit = { found = false }

          override def onJoin(sr: UnsafeRow, br: UnsafeRow): Boolean = {
            if (!found && boundCondition(joinedRow.withLeft(sr).withRight(br))) {
              foundVal = sr
              found = true
              true
            } else false
          }

          override protected def shouldSkipRest: Boolean = found

          override protected def rowToEmit: InternalRow = foundVal
        }
    }

  private def antiJoin(streamedIter: Iterator[InternalRow], setId: Long): Iterator[InternalRow] =
    new LookupJoinIterator(streamedIter, setId) {

      override protected def onNullKey(streamedRow: InternalRow): Unit =
        prepareNextRow(streamedRow)

      override protected def onBloomNegative(streamedRow: InternalRow): Unit =
        prepareNextRow(streamedRow)

      override protected def onMatches(batch: PBatch, buf: BBuffer): GenericRowIterator =
        new GenericRowIterator(batch, buf) {

          private var found: Boolean = _
          private var foundVal: UnsafeRow = _

          override protected def whichUr: UnsafeRow = buildUr

          override protected def onStreamedRow(sr: UnsafeRow): Unit = { found = false }

          override protected def onJoin(sr: UnsafeRow, br: UnsafeRow): Boolean = {
            if (!found && boundCondition(joinedRow.withLeft(sr).withRight(br))) {
              found = true
            }
            false
          }

          override protected def shouldSkipRest: Boolean = found

          override protected def onEndOfKey(sr: UnsafeRow): Boolean = {
            if (!found) { foundVal = sr; true }
            else false
          }

          override protected def rowToEmit: InternalRow = foundVal
        }
    }

  private def existenceJoin(
      streamedIter: Iterator[InternalRow],
      setId: Long): Iterator[InternalRow] = new LookupJoinIterator(streamedIter, setId) {

    private val existsRow = new GenericInternalRow(Array[Any](null))

    override protected def onNullKey(streamedRow: InternalRow): Unit = {
      existsRow.setBoolean(0, value = false)
      prepareNextRow(joinedRow(streamedRow, existsRow))
    }

    override protected def onBloomNegative(streamedRow: InternalRow): Unit = {
      existsRow.setBoolean(0, value = false)
      prepareNextRow(joinedRow(streamedRow, existsRow))
    }

    override protected def onMatches(batch: PBatch, buf: BBuffer): GenericRowIterator =
      new GenericRowIterator(batch, buf) {

        private var found: Boolean = _
        private var foundVal: UnsafeRow = _

        override protected def whichUr: UnsafeRow = buildUr

        override protected def onStreamedRow(sr: UnsafeRow): Unit = { found = false }

        override protected def onJoin(sr: UnsafeRow, br: UnsafeRow): Boolean = {
          if (!found && boundCondition(joinedRow.withLeft(sr).withRight(br))) {
            found = true
          }
          false
        }

        override protected def shouldSkipRest: Boolean = found

        override protected def onEndOfKey(sr: UnsafeRow): Boolean = {
          foundVal = sr
          existsRow.setBoolean(0, found)
          true
        }

        override protected def rowToEmit: InternalRow =
          joinedRow.withLeft(foundVal).withRight(existsRow)
      }
  }

  // probe-side batch
  private type PBatch = BufferedShardRowMap#KeyValueBatch
  // build-side buffer
  private type BBuffer = ManagedBuffer

  private abstract class GenericRowIterator(batch: PBatch, buffer: BBuffer)
      extends AutoCloseable {

    private val keyValuesIter = batch.multiValuesIterator()
    private var currValIter: util.Iterator[UnsafeRow] = _
    private var currVal: UnsafeRow = _
    private var currKeyIdx: Int = _
    private val buf = Unpooled.wrappedBuffer(buffer.nioByteBuffer())
    private val advanceRead = UnsafeRowUtils.makeAdvanceRead(whichUr, buf)

    assert(batch.getSetId == buf.readLong(), "ensure setId matches")
    assert(batch.getShard == buf.readInt(), "ensure shardId matches")

    protected def whichUr: UnsafeRow
    protected def onStreamedRow(sr: UnsafeRow): Unit = ()
    protected def onJoin(sr: UnsafeRow, br: UnsafeRow): Boolean
    protected def onEndOfKey(sr: UnsafeRow): Boolean = false
    protected def shouldSkipRest: Boolean = false
    protected def rowToEmit: InternalRow

    final def advanceNext(): Boolean = {
      var matched = false
      var forward = true
      while (!matched && forward) {
        if (currVal == null) {
          if (currValIter == null) {
            if (keyValuesIter.hasNext) {
              currValIter = keyValuesIter.next()
              currKeyIdx = buf.readerIndex()
            } else {
              forward = false
            }
          } else if (currValIter.hasNext) {
            currVal = currValIter.next()
            onStreamedRow(currVal)
            buf.readerIndex(currKeyIdx)
          } else {
            currValIter = null
          }
        } else {
          val blen = buf.readInt()
          if (blen == 0) {
            matched = onEndOfKey(currVal)
            currVal = null
          } else {
            if (shouldSkipRest) {
              buf.readerIndex(buf.readerIndex() + blen)
            } else {
              val buildRow = advanceRead(blen)
              matched = onJoin(currVal, buildRow)
            }
          }
        }
      }
      matched
    }

    final def getRow: InternalRow = rowToEmit

    override def close(): Unit = {
      batch.release()
      buffer.release()
    }
  }

  private abstract class LookupJoinIterator(streamedIter: Iterator[InternalRow], setId: Long)
      extends Iterator[InternalRow] {

    private val maxInFlightNum = conf.distributedMapJoinMaxInFlightNum
    private val keyGenerator: UnsafeProjection = UnsafeProjection.create(streamedBoundKeys)
    private val valueGenerator: UnsafeProjection = UnsafeProjection.create(streamedPlan.schema)
    private val shardGenerator =
      UnsafeProjection.create(
        HashPartitioning(streamedBoundKeys, numShards).partitionIdExpression :: Nil)

    private val bloom = streamedBloomFilter(setId)
    private val probeUr: UnsafeRow = new UnsafeRow(streamedPlan.schema.length)
    private val bufferedMap = {
      val mm = TaskContext.get().taskMemoryManager()
      val maxBatchSize = conf.distributedMapJoinMaxBatchSize
      val map =
        new BufferedShardRowMap(
          mm,
          setId,
          numShards,
          streamedBoundKeys.length,
          probeUr,
          maxBatchSize)
      TaskContext.get().addTaskCompletionListener[Unit](_ => map.free())
      map
    }

    // build-side response
    private sealed trait Lookup
    // lookup success
    private case class LookupSuccess(batch: PBatch, buffer: BBuffer) extends Lookup
    // lookup failure
    private case class LookupFailure(batch: PBatch, cause: Throwable) extends Lookup

    // lookup queue: [(probe-side batch, build-side response)]
    private val lookupQueue = new util.concurrent.LinkedBlockingQueue[Lookup]

    protected val buildUr: UnsafeRow = new UnsafeRow(buildOutput.length)
    protected val joinedRow: JoinedRow = new JoinedRow

    private var inputExhausted = false
    private var prepared = false
    private var nextRowVal: InternalRow = _
    private var numInFlight = 0
    private var lookupRowIter: GenericRowIterator = _

    override def hasNext: Boolean = {
      if (!prepared) {
        processNext()
      }
      prepared
    }

    override def next(): InternalRow = {
      if (!prepared && !hasNext) {
        throw QueryExecutionErrors.noSuchElementExceptionError()
      }
      prepared = false
      nextRowVal
    }

    protected def onNullKey(streamedRow: InternalRow): Unit = ()
    protected def onBloomNegative(streamedRow: InternalRow): Unit = ()
    protected def onMatches(batch: PBatch, buffer: BBuffer): GenericRowIterator

    protected final def prepareNextRow(row: InternalRow): Unit = {
      nextRowVal = row
      prepared = true
    }

    protected final def emitNullSupplied(
        streamedRow: InternalRow,
        nullBuildRow: InternalRow): Unit = {
      prepareNextRow(joinedRow.withLeft(streamedRow).withRight(nullBuildRow))
    }

    private def processNext(): Unit = {
      // iterate completed lookup if possible
      if (lookupRowIter != null) {
        iterateLookup()
      }

      var hasLookup = true
      while (!prepared && hasLookup) {
        val ele = lookupQueue.poll()
        if (ele == null) {
          hasLookup = false
        } else {
          pollLookup(ele)
          if (lookupRowIter != null) {
            iterateLookup()
          }
        }
      }

      // iterate input streamedIter if not exhausted
      while (!prepared && !inputExhausted && streamedIter.hasNext) {
        val streamedRow = streamedIter.next()
        val keyUr = keyGenerator(streamedRow)
        if (keyUr.anyNull) {
          onNullKey(streamedRow)
        } else if (!bloom.mightContain(keyUr.getBytes)) {
          onBloomNegative(streamedRow)
        } else {
          val shardId = shardGenerator(streamedRow).getInt(0)
          val valueUr = streamedRow match {
            case ur: UnsafeRow => ur
            case r => valueGenerator(r)
          }
          processLookup(shardId, keyUr, valueUr)
        }
      }

      if (!prepared) {
        if (!inputExhausted) {
          // input exhausted
          inputExhausted = true
          // flush tailing batches
          flushLookup(bufferedMap.tailingIterator())
        }
        // drain in-flight
        while (!prepared && numInFlight > 0) {
          pollLookup(lookupQueue.poll(200, util.concurrent.TimeUnit.MILLISECONDS))
          if (lookupRowIter != null) {
            iterateLookup()
          }
        }
      }
    }

    private def processLookup(shardId: Int, keyUr: UnsafeRow, valueUr: UnsafeRow): Unit = {
      // put shard row to buffer
      bufferedMap.putRow(
        shardId,
        keyUr.getBaseObject,
        keyUr.getBaseOffset,
        keyUr.getSizeInBytes,
        keyUr.hashCode(),
        valueUr.getBaseObject,
        valueUr.getBaseOffset,
        valueUr.getSizeInBytes)

      if (bufferedMap.hasPending) {
        // flush pending batches
        flushLookup(bufferedMap.pendingIterator())
      }

      // avoid piling up too many in-flight batches to prevent page memory exhaustion:
      // block until in-flight count drops below the limit before accepting more rows
      while (numInFlight >= maxInFlightNum) {
        pollLookup(lookupQueue.poll(200, util.concurrent.TimeUnit.MILLISECONDS))
        if (lookupRowIter != null) {
          iterateLookup()
        }
      }
    }

    private def flushLookup[T <: PBatch](iter: util.Iterator[T]): Unit = {
      val manager = SparkEnv.get.shardManager
      implicit val ec: ExecutionContextExecutorService = manager.lookupEc
      while (iter.hasNext) {
        val batch = iter.next()
        iter.remove()
        val future =
          manager.fetchRemoteBatch(setId, batch.getShard, () => batch.wrapKeysBuffer())

        future.foreach { buffer =>
          lookupQueue.put(LookupSuccess(batch, buffer))
        }

        future.failed.foreach { cause =>
          lookupQueue.put(LookupFailure(batch, cause))
        }

        // inc in-flight num
        numInFlight += 1
      }
    }

    private def pollLookup(look: Lookup): Unit = {
      look match {
        case LookupSuccess(batch, buffer) =>
          lookupRowIter = onMatches(batch, buffer)
          // dec in-flight num
          numInFlight -= 1
        case LookupFailure(batch, cause) =>
          // error occurred
          throw new SparkException(
            s"DistributedMapJoin batch lookup failed: ($setId, ${batch.getShard}) ",
            cause)
        case null => // do nothing
      }
    }

    private def iterateLookup(): Unit = {
      if (lookupRowIter.advanceNext()) {
        prepareNextRow(lookupRowIter.getRow)
      } else {
        // do close
        lookupRowIter.close()
        // reset row iter sentinel
        lookupRowIter = null
      }
    }
  }
}
