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

import io.netty.buffer.Unpooled

import org.apache.spark.network.buffer.{ManagedBuffer, NettyManagedBuffer}
import org.apache.spark.network.util.NettyUtils
import org.apache.spark.shard.{ShardLookupAdapter, ShardManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

private[spark] class HashedRelationAdapter extends ShardLookupAdapter {

  private val initialCapacity = 1 << 10 << 10
  private val alloc = NettyUtils.getSharedPooledByteBufAllocator(true, true)

  override def lookup(manager: ShardManager, reqMsg: ManagedBuffer): ManagedBuffer = {
    val keysBuf = Unpooled.wrappedBuffer(reqMsg.nioByteBuffer())
    // (setId) (shardId) (keys num-fields) [(key len) (key)]
    val setId = keysBuf.readLong()
    val shard = keysBuf.readInt()
    val numFields = keysBuf.readInt()
    val keyUr = new UnsafeRow(numFields)

    val rel = manager.getLocalValue[HashedRelation](setId, shard).asReadOnlyCopy()
    val valuesBuf = alloc.buffer(initialCapacity)
    // (setId) (shardId) [[(row len) (row)] (0)]
    valuesBuf.writeLong(setId)
    valuesBuf.writeInt(shard)

    val advanceRead = UnsafeRowUtils.makeAdvanceRead(keyUr, keysBuf)
    val advanceWrite = UnsafeRowUtils.makeAdvanceWrite(valuesBuf)

    while (keysBuf.isReadable) {
      val klen = keysBuf.readInt()
      val key = advanceRead(klen)
      val iter = rel.get(key)
      if (iter != null) {
        while (iter.hasNext) {
          val valueUr = iter.next().asInstanceOf[UnsafeRow]
          valuesBuf.writeInt(valueUr.getSizeInBytes)
          advanceWrite(valueUr)
        }
      }
      // separate rows from different keys
      valuesBuf.writeInt(0)
    }

    new NettyManagedBuffer(valuesBuf)
  }
}
