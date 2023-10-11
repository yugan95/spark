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

package org.apache.spark.sql.execution.datasources

import java.io.{FileNotFoundException, IOException}

import org.apache.parquet.io.ParquetDecodingException

import org.apache.spark.{Partition => RDDPartition, SparkEnv, SparkUpgradeException, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.{InputFileBlockHolder, RDD}
import org.apache.spark.scheduler.ExecutorCacheTaskLocation
import org.apache.spark.softaffinity.SoftAffinityManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.NextIterator

/**
 * A part (i.e. "block") of a single file that should be read, along with partition column values
 * that need to be prepended to each row.
 *
 * @param partitionValues value of partition columns to be prepended to each row.
 * @param filePath URI of the file to read
 * @param start the beginning offset (in bytes) of the block.
 * @param length number of bytes to read.
 * @param locations locality information (list of nodes that have the data).
 */
case class PartitionedFile(
    partitionValues: InternalRow,
    filePath: String,
    start: Long,
    length: Long,
    @transient locations: Array[String] = Array.empty,
    @transient var queryMetrics: Array[Long] = new Array[Long](4)) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}

/**
 * An RDD that scans a list of file partitions.
 */
class FileScanRDD(
    @transient private val sparkSession: SparkSession,
    readFunction: (PartitionedFile) => Iterator[InternalRow],
    @transient val filePartitions: Seq[FilePartition])
  extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private val ignoreCorruptFiles = sparkSession.sessionState.conf.ignoreCorruptFiles
  private val ignoreMissingFiles = sparkSession.sessionState.conf.ignoreMissingFiles
  private val collectQueryMetricsEnabled = sparkSession.sessionState.conf.collectQueryMetricsEnabled

  override def compute(split: RDDPartition, context: TaskContext): Iterator[InternalRow] = {
    val currTaskLocality = context.getLocalProperty("SAMetrics.taskLocality")
    val files = split.asInstanceOf[FilePartition].files
    val currFilePath = if (files.isEmpty) "bucket-read-empty-task" else files.head.filePath
    logInfo(s"SAMetrics=File ${currFilePath} running in task ${context.taskAttemptId()} " +
      s"on executor ${SparkEnv.get.executorId} with locality ${currTaskLocality}")
    val iterator = new Iterator[Object] with AutoCloseable {
      private val inputMetrics = context.taskMetrics().inputMetrics
      private val existingBytesRead = inputMetrics.bytesRead

      // Find a function that will return the FileSystem bytes read by this thread. Do this before
      // apply readFunction, because it might read some bytes.
      private val getBytesReadCallback =
        SparkHadoopUtil.get.getFSBytesReadOnThreadCallback()

      // We get our input bytes from thread-local Hadoop FileSystem statistics.
      // If we do a coalesce, however, we are likely to compute multiple partitions in the same
      // task and in the same thread, in which case we need to avoid override values written by
      // previous partitions (SPARK-13071).
      private def incTaskInputMetricsBytesRead(): Unit = {
        inputMetrics.setBytesRead(existingBytesRead + getBytesReadCallback())
      }

      private[this] val files = split.asInstanceOf[FilePartition].files.toIterator
      private[this] var currentFile: PartitionedFile = null
      private[this] var currentIterator: Iterator[Object] = null

      def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed. This logic is from
        // InterruptibleIterator, but we inline it here instead of wrapping the iterator in order
        // to avoid performance overhead.
        context.killTaskIfInterrupted()
        (currentIterator != null && currentIterator.hasNext) || nextIterator()
      }
      def next(): Object = {
        val nextElement = currentIterator.next()
        // TODO: we should have a better separation of row based and batch based scan, so that we
        // don't need to run this `if` for every record.
        val preNumRecordsRead = inputMetrics.recordsRead
        if (nextElement.isInstanceOf[ColumnarBatch]) {
          incTaskInputMetricsBytesRead()
          inputMetrics.incRecordsRead(nextElement.asInstanceOf[ColumnarBatch].numRows())
        } else {
          // too costly to update every record
          if (inputMetrics.recordsRead %
              SparkHadoopUtil.UPDATE_INPUT_METRICS_INTERVAL_RECORDS == 0) {
            incTaskInputMetricsBytesRead()
          }
          inputMetrics.incRecordsRead(1)
        }
        nextElement
      }

      private def readCurrentFile(): Iterator[InternalRow] = {
        try {
          readFunction(currentFile)
        } catch {
          case e: FileNotFoundException =>
            throw QueryExecutionErrors.readCurrentFileNotFoundError(e)
        }
      }

      /** Advances to the next file. Returns true if a new non-empty iterator is available. */
      private def nextIterator(): Boolean = {
        if (files.hasNext) {
          currentFile = files.next()
          logInfo(s"Reading File $currentFile")
          // Sets InputFileBlockHolder for the file block's information
          InputFileBlockHolder.set(currentFile.filePath, currentFile.start, currentFile.length)

          if (ignoreMissingFiles || ignoreCorruptFiles) {
            currentIterator = new NextIterator[Object] {
              // The readFunction may read some bytes before consuming the iterator, e.g.,
              // vectorized Parquet reader. Here we use lazy val to delay the creation of
              // iterator so that we will throw exception in `getNext`.
              private lazy val internalIter = readCurrentFile()

              override def getNext(): AnyRef = {
                try {
                  if (internalIter.hasNext) {
                    internalIter.next()
                  } else {
                    finished = true
                    null
                  }
                } catch {
                  case e: FileNotFoundException if ignoreMissingFiles =>
                    logWarning(s"Skipped missing file: $currentFile", e)
                    finished = true
                    null
                  // Throw FileNotFoundException even if `ignoreCorruptFiles` is true
                  case e: FileNotFoundException if !ignoreMissingFiles => throw e
                  case e @ (_: RuntimeException | _: IOException) if ignoreCorruptFiles =>
                    logWarning(
                      s"Skipped the rest of the content in the corrupted file: $currentFile", e)
                    finished = true
                    null
                }
              }

              override def close(): Unit = {}
            }
          } else {
            currentIterator = readCurrentFile()
            if (collectQueryMetricsEnabled) {
              try {
                if (currentFile != null
                  && currentFile.queryMetrics != null) {
                  inputMetrics.incFooterReadTime(currentFile.queryMetrics(3))
                  inputMetrics.incFooterReadNumber(1L)
                  if (currentFile.queryMetrics(0) > 0) {
                    inputMetrics.incTotalBloomBlocks(currentFile.queryMetrics(0))
                    inputMetrics.incSkipBloomBlocks(currentFile.queryMetrics(1))
                    inputMetrics.incSkipRows(currentFile.queryMetrics(2))
                  }
                }
              } catch {
                case e: Throwable =>
                  logWarning("Error when collect query status", e)
              }
            }
          }

          try {
            hasNext
          } catch {
            case e: SchemaColumnConvertNotSupportedException =>
              throw QueryExecutionErrors.unsupportedSchemaColumnConvertError(
                currentFile.filePath, e.getColumn, e.getLogicalType, e.getPhysicalType, e)
            case e: ParquetDecodingException =>
              if (e.getCause.isInstanceOf[SparkUpgradeException]) {
                throw e.getCause
              } else if (e.getMessage.contains("Can not read value at")) {
                throw QueryExecutionErrors.cannotReadParquetFilesError(e)
              }
              throw e
          }
        } else {
          currentFile = null
          InputFileBlockHolder.unset()
          false
        }
      }

      def collectQueryMetrics() = {
        if (currentIterator != null && currentIterator.isInstanceOf[RecordReaderIterator[Object]]) {
          val queryMetrics = currentIterator.asInstanceOf[RecordReaderIterator[Object]]
            .getParquetQueryMetrics()
          if (queryMetrics.getTotalPagesCount > 0) {
            inputMetrics.incTotalPagesCount(queryMetrics.getTotalPagesCount);
            inputMetrics.incFilteredPagesCount(queryMetrics.getFilteredPagesCount);
            inputMetrics.incAfterFilterPagesCount(queryMetrics.getAfterFilterPagesCount);
          }
        }
      }

      override def close(): Unit = {
        collectQueryMetrics()
        incTaskInputMetricsBytesRead()
        InputFileBlockHolder.unset()
      }
    }

    // Register an on-task-completion callback to close the input stream.
    context.addTaskCompletionListener[Unit](_ => iterator.close())

    iterator.asInstanceOf[Iterator[InternalRow]] // This is an erasure hack.
  }

  override protected def getPartitions: Array[RDDPartition] = filePartitions.toArray

  override protected def getPreferredLocations(split: RDDPartition): Seq[String] = {
    val filePartition = split.asInstanceOf[FilePartition]
    val expectedTargets = filePartition.preferredLocations()
    val files = filePartition.files

    // logInfo(s"The expected target hosts are ${expectedTargets.mkString(",")}, " +
    //   s"calculated by file ${files.mkString(",")}")
    if (!files.isEmpty && SoftAffinityManager.usingSoftAffinity()
      && !SoftAffinityManager.checkTargetHosts(expectedTargets)) {
      // if there is no host in the node list which are executors running on,
      // using SoftAffinityManager to generate target executors.
      // Only using the first file to calculate the target executors
      val expectedExecutors = SoftAffinityManager.askExecutors(files.head.filePath)
        .map( target => {
          ExecutorCacheTaskLocation(target._2, target._1).toString
        })
      if (expectedExecutors.isEmpty) {
        expectedTargets
      } else {
        logInfo(s"SAMetrics=File ${files.head.filePath} - " +
          s"the expected executors are ${expectedExecutors.mkString("_")}")
        expectedExecutors
      }
    } else {
      expectedTargets
    }
  }

  def partitionFilesTotalLength: Long = {
    filePartitions.map(_.files.map(_.length).sum).sum
  }

}
