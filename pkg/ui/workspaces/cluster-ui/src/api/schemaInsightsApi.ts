// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {executeSql, SqlExecutionRequest, SqlTxnResult} from "./sqlApi";
import {InsightRecommendation, InsightType} from "../insightsTable/insightsTable";

type ClusterIndexUsageStatistic = {
  table_id?: number;
  index_id?: number;
  last_read?: string;
  index_name?: string;
  table_name?: string;
  database_id?: number;
  database_name?: string;
};

// TODO(thomas): remove
type SHOWDATABASES = {
  database_name: string,
  owner: string,
  primary_region: string | null,
  regions: string[],
  survival_goal: string | null
};

type SchemaInsightQuery<RowType> = {
  name: InsightType;
  query: string;
  toState: (
    response: SqlTxnResult<RowType>,
  ) => InsightRecommendation[];
}

enum SchemaInsightStatementOrdering {
  ClusterIndexUsageStatistics = 1,
  SHOWDATABASES
}

type ClusterIndexUsageStatistics = ClusterIndexUsageStatistic[];
type SchemaInsight = ClusterIndexUsageStatistics | SHOWDATABASES;
type SchemaInsightQueries<RowType> = SchemaInsightQuery<RowType>[];

const schemaInsightQueries: SchemaInsightQueries<SchemaInsight> = []

const actualIndexStatsQuery = `SELECT
                                 us.table_id,
                                 us.index_id,
                                 us.last_read,
                                 ti.index_name,
                                 t.name as table_name,
                                 t.parent_id as database_id,
                                 t.database_name
                               FROM "".crdb_internal.index_usage_statistics AS us
                                      JOIN "".crdb_internal.table_indexes as ti ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
                                      JOIN "".crdb_internal.tables as t ON t.table_id = ti.descriptor_id and t.name = ti.descriptor_name
                               WHERE t.database_name != 'system' AND ti.index_type != 'primary';`

const clusterWideIndexStatsQuery: SchemaInsightQuery<ClusterIndexUsageStatistic> = {
  name: "DROP_INDEX",
  query: `SELECT
            us.table_id,
            us.index_id,
            us.last_read,
            ti.index_name,
            t.name as table_name,
            t.parent_id as database_id,
            t.database_name
          FROM "".crdb_internal.index_usage_statistics AS us
                 JOIN "".crdb_internal.table_indexes as ti ON us.index_id = ti.index_id AND us.table_id = ti.descriptor_id
                 JOIN "".crdb_internal.tables as t ON t.table_id = ti.descriptor_id and t.name = ti.descriptor_name
          WHERE t.database_name != 'system' AND ti.index_type != 'primary';`,
  toState: clusterWideIndexStatsToState
}

// TODO(thomas): finish
function clusterWideIndexStatsToState(
  txn_result: SqlTxnResult<ClusterIndexUsageStatistic>,
): InsightRecommendation[] {
  if (!txn_result.rows) {
    return []
  }

  return [];
}

export function getSchemaInsights() {
  const request: SqlExecutionRequest = {
    statements: [
      {
        sql: actualIndexStatsQuery
      },
      {
        sql: "SHOW DATABASES"
      }
    ],
    execute: true,
  };
  return executeSql<SchemaInsight>(request).then(
    result => {
      if (
        result.execution.txn_results.length === 0
      ) {
        // No data.
        console.log("no txn results", []);
        return [];
      }

      console.log("txn results", result.execution.txn_results);
    },
  );
}
