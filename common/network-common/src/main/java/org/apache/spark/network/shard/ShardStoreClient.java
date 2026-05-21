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

package org.apache.spark.network.shard;

import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.network.client.TransportClientFactory;
import org.apache.spark.network.util.TransportConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

public abstract class ShardStoreClient implements Closeable {
    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected volatile TransportClientFactory clientFactory;
    protected String appId;
    private String appAttemptId;
    protected TransportConf transportConf;

    protected void checkInit() {
        assert appId != null : "Called before init()";
    }

    public abstract void fetchBatch(
            String host,
            int port,
            ManagedBuffer reqMsg,
            ShardLookupListener listener);

    public void setAppAttemptId(String appAttemptId) {
        this.appAttemptId = appAttemptId;
    }

    public String getAppAttemptId() {
        return this.appAttemptId;
    }
}
