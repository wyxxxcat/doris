// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_frontend", "nonConcurrent") {
    def res = sql """SHOW FRONTENDS DISKS"""
    assertTrue(res.size() != 0)

    def res2 = sql """SHOW FRONTENDS Disks"""
    assertTrue(res2.size() != 0)

    if (isCloudMode()) {
        // In the test_sql_mode_node_mgr regression case, there is already a similar and more complex case. This case is redundant. Additionally, there is a 5-minute limit for dropping FE on the cloud.
        // so ignore it in cloud
        return;
    }
    def address = "127.0.0.1"
    def notExistPort = 12345

    for (int i = 0; i < 2; i++) {
        def result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM ADD FOLLOWER "${address}:${notExistPort}";"""
        waitAddFeFinished(address, notExistPort);
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM DROP FOLLOWER "${address}:${notExistPort}";"""
        waitDropFeFinished(address, notExistPort);
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM ADD OBSERVER "${address}:${notExistPort}";"""
        waitAddFeFinished(address, notExistPort);
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")

        sql """ALTER SYSTEM DROP OBSERVER "${address}:${notExistPort}";"""
        waitDropFeFinished(address, notExistPort);
        result = sql """SHOW FRONTENDS;"""
        logger.debug("result:${result}")
    }
}
