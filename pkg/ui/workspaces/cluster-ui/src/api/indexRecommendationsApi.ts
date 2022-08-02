// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
  Ok, so what do we want here:
    - cluster-wide index recommendations

  What does the response obj look like?
  [
    {
      table_id int,
      index_id int,
      last_read timestamp / DateTime,
      index_name string,
      table_name string,
      database_id int,
      database_name string
    }
  ]


SELECT
    us.table_id,
    us.index_id,
    us.last_read,
    ti.index_name,
    t.name as table_name,
    t.parent_id as database_id,
    t.database_name
FROM crdb_internal.index_usage_statistics AS us
JOIN crdb_internal.table_indexes as ti ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
JOIN crdb_internal.tables as t ON t.table_id = ti.descriptor_id and t.name = ti.descriptor_name

 */

import {cockroach} from "@cockroachlabs/crdb-protobuf-client";
import Timestamp = cockroach.util.hlc.Timestamp;
import {executeSql, SqlExecutionRequest} from "./sqlApi";

type ClusterIndexUsageStatistic = {
  table_id?: Number;
  index_id?: Number;
  last_read?: Timestamp;
  index_name?: String;
  table_name?: String;
  database_id?: Number;
  database_name?: String;
};

type ClusterIndexUsageStatistics = ClusterIndexUsageStatistic[];

/**
 * getClusterIndexUsageStatistics returns cluster-wide index usage statistics.
 */
export function getClusterIndexUsageStatistics(): Promise<ClusterIndexUsageStatistics> {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: `
          SELECT
            us.table_id,
            us.index_id,
            us.last_read,
            ti.index_name,
            t.name as table_name,
            t.parent_id as database_id,
            t.database_name
          FROM crdb_internal.index_usage_statistics AS us
                 JOIN crdb_internal.table_indexes as ti ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
                 JOIN crdb_internal.tables as t ON t.table_id = ti.descriptor_id and t.name = ti.descriptor_name`,
      },
    ],
    execute: true,
  };
  return executeSql<ClusterIndexUsageStatistics>(request).then(result => {
    if (
      result.execution.txn_results.length === 0 ||
      !result.execution.txn_results[0].rows
    ) {
      // No data.
      return [];
    }
  });
}
