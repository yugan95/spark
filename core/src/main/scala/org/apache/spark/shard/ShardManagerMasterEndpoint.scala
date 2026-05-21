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

package org.apache.spark.shard

import java.util.{HashMap => JHashMap}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.{IsolatedThreadSafeRpcEndpoint, RpcCallContext, RpcEndpointRef, RpcEnv}
import org.apache.spark.shard.ShardManagerMessages._
import org.apache.spark.util.ThreadUtils

private[spark] class ShardManagerMasterEndpoint(
    val rpcEnv: RpcEnv,
    val isLocal: Boolean,
    conf: SparkConf,
    shardManagerInfo: mutable.Map[ShardManagerId, ShardManagerInfo],
    isDriver: Boolean)
    extends IsolatedThreadSafeRpcEndpoint
    with Logging {

  private val nextShardSetId = new AtomicLong(0)

  private val shardSetInfo = new mutable.HashMap[Long, ShardSetInfo]
  private val shardSetLocations =
    new JHashMap[Long, JHashMap[Int, mutable.HashSet[ShardManagerId]]]

  private implicit val askEc: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(
      ThreadUtils.newDaemonCachedThreadPool("shard-manager-ask-thread-pool", 8))

  override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
    case RegisterShardManager(id, endpoint) =>
      context.reply(register(id, endpoint))

    case NewShardSet(numShards, replicaCount) =>
      context.reply(newShardSet(numShards, replicaCount))

    case UpdateShardInfo(shardManagerId, setId, shardId) =>
      updateShardInfo(shardManagerId, setId, shardId)
      context.reply(true)

    case InstallReplicaSet(setId, shardId) =>
      installReplicaToWorkers(setId, shardId)
      context.reply(true)

    case GetLocations(setId, shardId) =>
      context.reply(getLocations(setId, shardId))

    case RemoveExecutor(execId) =>
      removeExecutor(execId)
      context.reply(true)

    case StopShardManagerMaster =>
      context.reply(true)
      stop()
  }

  private def register(
      idWithoutTopologyInfo: ShardManagerId,
      managerEndpoint: RpcEndpointRef): ShardManagerId = {
    val id = ShardManagerId(
      idWithoutTopologyInfo.executorId,
      idWithoutTopologyInfo.host,
      idWithoutTopologyInfo.port,
      None)
    shardManagerInfo(id) = new ShardManagerInfo(id, managerEndpoint)
    id
  }

  private def removeShardManager(shardManagerId: ShardManagerId): Unit = {
    val info = shardManagerInfo(shardManagerId)
    shardManagerInfo.remove(shardManagerId)

    val iterator = info.shards.entrySet().iterator()
    while (iterator.hasNext) {
      val entry = iterator.next()
      val setId = entry.getKey
      val shardLocations = shardSetLocations.get(setId)
      if (shardLocations != null) {
        val valueIterator = entry.getValue.iterator
        while (valueIterator.hasNext) {
          val shardId = valueIterator.next()
          val locations = shardLocations.get(shardId)
          if (locations != null) {
            locations -= shardManagerId
            if (locations.isEmpty) {
              shardLocations.remove(shardId)
              logWarning(s"No more shard replicas available for ($setId, $shardId)")
            } else {
              // proactively replicate here.
              installReplicaToWorkers(setId, shardId)
            }
          }
        }
        if (shardLocations.isEmpty) {
          shardSetLocations.remove(setId)
        }
      }
    }

    logInfo(s"Removing shard manager $shardManagerId")
  }

  private def newShardSet(numShards: Int, replicaCount: Int): Long = {
    val setId = nextShardSetId.getAndIncrement()
    shardSetInfo(setId) = new ShardSetInfo(numShards, replicaCount)
    setId
  }

  private def updateShardInfo(shardManagerId: ShardManagerId, setId: Long, shardId: Int): Unit = {
    if (!shardManagerInfo.contains(shardManagerId)) {
      return
    }

    shardManagerInfo(shardManagerId).updateShardInfo(setId, shardId)

    var locations: mutable.HashSet[ShardManagerId] = null
    if (shardSetLocations.containsKey(setId)) {
      if (shardSetLocations.get(setId).containsKey(shardId)) {
        locations = shardSetLocations.get(setId).get(shardId)
      } else {
        locations = new mutable.HashSet[ShardManagerId]
        shardSetLocations.get(setId).put(shardId, locations)
      }
    } else {
      locations = new mutable.HashSet[ShardManagerId]
      val shardLocations = new JHashMap[Int, mutable.HashSet[ShardManagerId]]
      shardLocations.put(shardId, locations)
      shardSetLocations.put(setId, shardLocations)
    }
    locations.add(shardManagerId)
  }

  private def installReplicaToWorkers(setId: Long, shardId: Int): Unit = {
    shardSetInfo
      .get(setId)
      .foreach { setInfo =>
        val targetReplica = setInfo.replicaCount
        val currentHolders = getLocations(setId, shardId)

        val have = currentHolders.size
        val need = math.max(0, targetReplica - have)

        val perExecSetLoad = mutable.HashMap.empty[String, Int].withDefaultValue(0)
        val perHostSetLoad = mutable.HashMap.empty[String, Int].withDefaultValue(0)

        def inc(m: scala.collection.mutable.Map[String, Int], k: String, d: Int = 1): Unit = {
          m.update(k, m.getOrElse(k, 0) + d)
        }
        def bumpLoads(smi: ShardManagerId, execDelta: Int = 1, hostDelta: Int = 1): Unit = {
          inc(perExecSetLoad, smi.executorId, execDelta)
          inc(perHostSetLoad, smi.host, hostDelta)
        }

        Option(shardSetLocations.get(setId)).foreach(locMap =>
          locMap.values().asScala.foreach(holderSet => holderSet.foreach(smi => bumpLoads(smi))))

        val usedHosts = mutable.HashSet.empty[String]
        currentHolders.foreach(smi => usedHosts += smi.host)

        val chosen = mutable.ArrayBuffer.empty[ShardManagerId]
        val chosenExecIds = mutable.HashSet.empty[String]
        val rng = new Random(setId ^ (shardId.toLong << 32))

        def score(smi: ShardManagerId): Long = {
          val execLoad = perExecSetLoad(smi.executorId)
          val hostLoad = perHostSetLoad(smi.host)
          val sameHost = if (usedHosts.contains(smi.host)) 100000L else 0L
          execLoad * 10 + hostLoad * 5 + sameHost + (rng.nextInt(256) & 0xff).toLong
        }

        val currentExecIds = currentHolders.map(_.executorId).toSet
        lazy val candidates =
          shardManagerInfo.keys.filterNot(smi => currentExecIds.contains(smi.executorId))

        var remaining = need
        while (remaining > 0) {
          val legals = candidates.filterNot(smi => chosenExecIds.contains(smi.executorId))
          if (legals.isEmpty) {
            remaining = 0
          } else {
            val best = legals.minBy(score)
            chosen += best
            chosenExecIds += best.executorId
            usedHosts += best.host
            bumpLoads(best)
            remaining -= 1
          }
        }

        chosen.foreach { smi =>
          shardManagerInfo.get(smi).foreach { sm =>
            sm.managerEndpoint.ask[Boolean](InstallReplica(setId, shardId))
          }
        }
      }
  }

  private def removeExecutor(execId: String): Unit = {
    logInfo("Trying to remove executor " + execId + " from ShardManagerMaster.")
     val ids = shardManagerInfo.keys.filter( _.executorId.equals(execId)).toSeq
    ids.foreach(removeShardManager)
  }

  private def getLocations(setId: Long, shardId: Int): Seq[ShardManagerId] = {
    if (!shardSetLocations
        .containsKey(setId) || !shardSetLocations.get(setId).containsKey(shardId)) {
      Seq.empty
    } else shardSetLocations.get(setId).get(shardId).toSeq
  }

  override def onStop(): Unit = {
    askEc.shutdown()
  }
}

private[spark] class ShardSetInfo(val numShards: Int, val replicaCount: Int) extends Logging {}

private[spark] class ShardManagerInfo(
    val shardManagerId: ShardManagerId,
    val managerEndpoint: RpcEndpointRef)
    extends Logging {
  private val _shards = new JHashMap[Long, mutable.HashSet[Int]]()

  def updateShardInfo(setId: Long, id: Int): Unit = {
    var ids: mutable.HashSet[Int] = null
    if (_shards.containsKey(setId)) {
      ids = _shards.get(setId)
    } else {
      ids = new mutable.HashSet[Int]()
      _shards.put(setId, ids)
    }
    ids.add(id)
  }

  def shards: JHashMap[Long, mutable.HashSet[Int]] = _shards
}
