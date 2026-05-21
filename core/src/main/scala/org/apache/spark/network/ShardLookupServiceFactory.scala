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

package org.apache.spark.network

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.network.netty.NettyShardLookupService
import org.apache.spark.rpc.RpcEndpointRef
import org.apache.spark.util.Utils

/**
 * Factory for creating ShardLookupService instances.
 *
 * When Gluten + Velox backend is enabled, creates GlutenShardLookupService via reflection
 * to avoid AliSpark hard-depending on Gluten jar. Otherwise creates NettyShardLookupService.
 */
private[spark] object ShardLookupServiceFactory extends Logging {

  private val GLUTEN_VELOX_BACKEND_KEY = "spark.gluten.enabled"
  private val GLUTEN_SHARD_LOOKUP_SERVICE_CLASS =
    "org.apache.spark.network.GlutenShardLookupService"

  def create(
      conf: SparkConf,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      numCores: Int,
      masterEndpoint: RpcEndpointRef): ShardLookupService = {
    if (isGlutenVeloxEnabled(conf)) {
      try {
        logInfo("Creating GlutenShardLookupService for native shard lookup")
        Utils.classForName(GLUTEN_SHARD_LOOKUP_SERVICE_CLASS)
          .getDeclaredConstructor(
            classOf[SparkConf], classOf[String], classOf[String], classOf[Int])
          .newInstance(conf, bindAddress, advertiseAddress, port.asInstanceOf[AnyRef])
          .asInstanceOf[ShardLookupService]
      } catch {
        case e: Exception =>
          logWarning(
            s"Failed to create GlutenShardLookupService, falling back to Netty", e)
          createNettyService(conf, bindAddress, advertiseAddress, port, numCores, masterEndpoint)
      }
    } else {
      createNettyService(conf, bindAddress, advertiseAddress, port, numCores, masterEndpoint)
    }
  }

  private def createNettyService(
      conf: SparkConf,
      bindAddress: String,
      advertiseAddress: String,
      port: Int,
      numCores: Int,
      masterEndpoint: RpcEndpointRef): ShardLookupService = {
    new NettyShardLookupService(conf, bindAddress, advertiseAddress, port,
      numCores, masterEndpoint)
  }

  private def isGlutenVeloxEnabled(conf: SparkConf): Boolean = {
    conf.getBoolean(GLUTEN_VELOX_BACKEND_KEY, defaultValue = false)
  }
}
