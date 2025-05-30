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


suite("test_index_match_regexp", "nonConcurrent"){
    def indexTbName1 = "test_index_match_regexp"

    sql "DROP TABLE IF EXISTS ${indexTbName1}"

    sql """
      CREATE TABLE ${indexTbName1} (
      `@timestamp` int(11) NULL COMMENT "",
      `clientip` varchar(20) NULL COMMENT "",
      `request` text NULL COMMENT "",
      `status` int(11) NULL COMMENT "",
      `size` int(11) NULL COMMENT "",
      INDEX request_idx (`request`) USING INVERTED PROPERTIES("parser" = "english") COMMENT ''
      ) ENGINE=OLAP
      DUPLICATE KEY(`@timestamp`)
      COMMENT "OLAP"
      DISTRIBUTED BY RANDOM BUCKETS 1
      PROPERTIES (
      "replication_allocation" = "tag.location.default: 1"
      );
    """

    def load_httplogs_data = {table_name, label, read_flag, format_flag, file_name, ignore_failure=false,
                        expected_succ_rows = -1, load_to_single_tablet = 'true' ->
        
        // load the json data
        streamLoad {
            table "${table_name}"
            
            // set http request header params
            set 'label', label + "_" + UUID.randomUUID().toString()
            set 'read_json_by_line', read_flag
            set 'format', format_flag
            file file_name // import json file
            time 10000 // limit inflight 10s
            if (expected_succ_rows >= 0) {
                set 'max_filter_ratio', '1'
            }

            // if declared a check callback, the default check condition will ignore.
            // So you must check all condition
            check { result, exception, startTime, endTime ->
		        if (ignore_failure && expected_succ_rows < 0) { return }
                    if (exception != null) {
                        throw exception
                    }
                    log.info("Stream load result: ${result}".toString())
                    def json = parseJson(result)
                    assertEquals("success", json.Status.toLowerCase())
                    if (expected_succ_rows >= 0) {
                        assertEquals(json.NumberLoadedRows, expected_succ_rows)
                    } else {
                        assertEquals(json.NumberTotalRows, json.NumberLoadedRows + json.NumberUnselectedRows)
                        assertTrue(json.NumberLoadedRows > 0 && json.LoadBytes > 0)
                }
            }
        }
    }

    try {
        load_httplogs_data.call(indexTbName1, 'test_index_match_regexp', 'true', 'json', 'documents-1000.json')

        sql "sync"
        sql """ set enable_common_expr_pushdown = true; """
        GetDebugPoint().enableDebugPointForAllBEs("VMatchPredicate.execute")

        qt_sql """ select count() from test_index_match_regexp where request match_regexp ''; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^h'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^team'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp 's\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp 'er\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '.*tickets.*'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp 'nonexistence'; """

        sql """ set inverted_index_max_expansions = 1; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp 'b'; """
        
        sql """ set inverted_index_max_expansions = 50; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp 'b'; """

        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^GET\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^images\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^french\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^HTTP\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^hm_'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^jpg\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^gif\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^html\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^tickets\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^nav_'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^splash\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^body\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^quest\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^venue\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^hosts\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^tck_'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^arw\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^brdl\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^nbg\$'; """
        qt_sql """ select count() from test_index_match_regexp where request match_regexp '^inet\$'; """

        try {
            GetDebugPoint().enableDebugPointForAllBEs("RegexpQuery.get_regex_prefix")

            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^GET\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^images\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^french\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^HTTP\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^hm_'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^jpg\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^gif\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^html\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^tickets\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^nav_'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^splash\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^body\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^quest\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^venue\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^hosts\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^tck_'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^arw\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^brdl\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^nbg\$'; """
            qt_sql """ select count() from test_index_match_regexp where request match_regexp '^inet\$'; """
        } finally {
            GetDebugPoint().disableDebugPointForAllBEs("RegexpQuery.get_regex_prefix")
        }
    } finally {
        sql """ set inverted_index_max_expansions = 50; """
        GetDebugPoint().disableDebugPointForAllBEs("VMatchPredicate.execute")
    }
}