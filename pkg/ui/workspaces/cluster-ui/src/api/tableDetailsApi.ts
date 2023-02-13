// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  executeInternalSql,
  formatApiResult,
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  SqlApiResponse,
  SqlExecutionErrorMessage,
  SqlExecutionRequest,
  SqlStatement,
  SqlTxnResult,
} from "./sqlApi";
import moment from "moment";
import { fromHexString, stripLeadingHexMarker, withTimeout } from "./util";
import { Format, Identifier, Join, SQL } from "./safesql";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { IndexUsageStatistic, recommendDropUnusedIndex } from "../insights";

const { ZoneConfig } = cockroach.config.zonepb;
const { ZoneConfigurationLevel } = cockroach.server.serverpb;
type ZoneConfigType = cockroach.config.zonepb.ZoneConfig;
type ZoneConfigLevelType = cockroach.server.serverpb.ZoneConfigurationLevel;

export function newTableDetailsResponse(): TableDetailsResponse {
  return {
    id_resp: { table_id: "" },
    create_stmt_resp: { statement: "" },
    grants_resp: { grants: [] },
    schema_details: { num_columns: 0, num_indexes: 0 },
    zone_config_resp: {
      configure_zone_statement: "",
      zone_config: new ZoneConfig({
        inherited_constraints: true,
        inherited_lease_preferences: true,
      }),
      zone_config_level: ZoneConfigurationLevel.CLUSTER,
    },
    heuristics_details: { stats_last_created_at: null },
    stats: {
      ranges_data: {
        range_count: 0,
        live_bytes: 0,
        total_bytes: 0,
        live_percentage: 0,
        // Note: we are currently populating this with replica ids which do not map 1 to 1
        replica_count: 0,
        node_count: 0,
        node_ids: [],
      },
      pebble_data: {
        approximate_disk_bytes: 0,
      },
      index_stats: {
        has_index_recommendations: false,
      },
    },
  };
}

type TableDetailsResponse = {
  id_resp: TableIdResponse;
  create_stmt_resp: TableCreateStatementResponse;
  grants_resp: TableGrantsResponse;
  schema_details: TableSchemaDetails;
  zone_config_resp: TableZoneConfigResponse;
  heuristics_details: TableHeuristicsDetails;
  stats: TableDetailsStats;
  error?: SqlExecutionErrorMessage;
};

// Table ID.
type TableIdResponse = TableIdRow & {
  error?: Error;
};
type TableIdRow = {
  table_id: string;
};
const getTableId: TableDetailsQuery<TableIdRow> = {
  createStmt: (dbName, tableName) => {
    return {
      sql: `SELECT $1::regclass::oid`,
      arguments: [tableName],
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableIdRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      resp.id_resp.table_id = txn_result.rows[0].table_id;
    } else {
      txn_result.error = new Error("fetchTableId: unexpected empty results");
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Table create statement.
type TableCreateStatementResponse = TableCreateStatementRow & { error?: Error };
type TableCreateStatementRow = { statement: string };
const getTableCreateStatement: TableDetailsQuery<TableCreateStatementRow> = {
  createStmt: (dbName, tableName) => {
    const fullyQualifiedName = Join(
      [new Identifier(dbName), "public", new Identifier(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(`SELECT create_statement FROM [SHOW CREATE %1]`, [
        fullyQualifiedName,
      ]),
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableCreateStatementRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      resp.create_stmt_resp.statement = txn_result.rows[0].statement;
    } else {
      txn_result.error = new Error(
        "getTableCreateStatement: unexpected empty results",
      );
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Table grants.
type TableGrantsResponse = {
  grants: TableGrantsRow[];
  error?: Error;
};
type TableGrantsRow = {
  user: string;
  privileges: string[];
};
// TODO(thomas): currently, this is wrong. Need to properly handle schema name.
//  Table name is schemaName.tableName, need to split and identify or something.
const getTableGrants: TableDetailsQuery<TableGrantsRow> = {
  createStmt: (dbName, tableName) => {
    return {
      sql: `SELECT grantee, privilege_type from information_schema.table_privileges where table_catalog = $1 and table_schema = 'public' and table_name = $3`,
      arguments: [dbName, tableName],
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableGrantsRow>,
    resp: TableDetailsResponse,
  ) => {
    if (txn_result.rows) {
      resp.grants_resp.grants = txn_result.rows;
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Table schema details.
type TableSchemaDetails = TableSchemaDetailsRow & {
  error?: Error;
};
type TableSchemaDetailsRow = {
  num_columns: number;
  num_indexes: number;
};
// TODO(thomas): currently, this is wrong. Need to properly handle schema name.
//  Table name is schemaName.tableName, need to split and identify or something.
const getTableSchemaDetails: TableDetailsQuery<TableSchemaDetailsRow> = {
  createStmt: (dbName, tableName) => {
    return {
      sql: `SELECT 
            count(distinct(column_name)) as num_columns, 
            count(distinct(index_name)) as num_indexes 
        FROM information_schema.statistics where table_catalog = $1 and table_schema = 'public' and table_name = $2`,

      arguments: [dbName, tableName],
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableSchemaDetailsRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      resp.schema_details.num_columns = txn_result.rows[0].num_columns;
      resp.schema_details.num_indexes = txn_result.rows[0].num_indexes;
    }
    if (txn_result.error) {
      resp.schema_details.error = txn_result.error;
    }
  },
};

// Table zone config.
type TableZoneConfigResponse = {
  configure_zone_statement: string;
  zone_config: ZoneConfigType;
  zone_config_level: ZoneConfigLevelType;
  error?: Error;
};
type TableZoneConfigStatementRow = { raw_config_sql: string };
// TODO(thomas): currently, this is wrong. Need to properly handle schema name.
//  Table name is schemaName.tableName, need to split and identify or something.
const getTableZoneConfigStmt: TableDetailsQuery<TableZoneConfigStatementRow> = {
  createStmt: (dbName, tableName) => {
    const fullyQualifiedName = Join(
      [new Identifier(dbName), "public", new Identifier(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(
        `SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FOR TABLE %1]`,
        [fullyQualifiedName],
      ),
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableZoneConfigStatementRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      resp.zone_config_resp.configure_zone_statement =
        txn_result.rows[0].raw_config_sql;
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};
type TableZoneConfigRow = {
  database_zone_config_bytes: string;
  table_zone_config_bytes: string;
};
const getTableZoneConfig: TableDetailsQuery<TableZoneConfigRow> = {
  createStmt: (dbName, tableName) => {
    return {
      sql: `WITH 
             dbId AS (SELECT crdb_internal.get_database_id($1) as database_id),
             tableId AS (SELECT $2::regclass::int as table_id)
            SELECT crdb_internal.get_zone_config((SELECT database_id FROM dbId)) as database_zone_config_bytes,
                   crdb_internal.get_zone_config((SELECT table_id FROM tableId)) as table_zone_config_bytes`,
      arguments: [dbName, tableName],
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableZoneConfigRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      const zoneConfigData = {
        hexString: "",
        configLevel: ZoneConfigurationLevel.CLUSTER,
      };
      // Check that database_zone_config_bytes is not null
      // and not empty.
      if (
        txn_result.rows[0].database_zone_config_bytes &&
        txn_result.rows[0].database_zone_config_bytes.length !== 0
      ) {
        zoneConfigData.hexString =
          txn_result.rows[0].database_zone_config_bytes;
        zoneConfigData.configLevel = ZoneConfigurationLevel.DATABASE;
      }
      // Fall back to table_zone_config_bytes if we don't have database_zone_config_bytes.
      if (
        zoneConfigData.hexString === "" &&
        txn_result.rows[0].table_zone_config_bytes &&
        txn_result.rows[0].table_zone_config_bytes.length !== 0
      ) {
        zoneConfigData.hexString = txn_result.rows[0].table_zone_config_bytes;
        zoneConfigData.configLevel = ZoneConfigurationLevel.TABLE;
      }
      // Try to decode the zone config bytes response.
      try {
        // Zone config bytes in the row are represented as a hex string,
        // strip the leading hex marker so we can parse it later.
        zoneConfigData.hexString = stripLeadingHexMarker(
          zoneConfigData.hexString,
        );
        // Parse the bytes from the hex string.
        const zoneConfigBytes = fromHexString(zoneConfigData.hexString);
        // Decode the bytes using ZoneConfig protobuf.
        resp.zone_config_resp.zone_config = ZoneConfig.decode(
          new Uint8Array(zoneConfigBytes),
        );
        resp.zone_config_resp.zone_config_level = zoneConfigData.configLevel;
      } catch (e) {
        // Catch and assign the error if we encounter one decoding.
        resp.zone_config_resp.error = e;
        resp.zone_config_resp.zone_config_level =
          ZoneConfigurationLevel.UNKNOWN;
      }
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Table heuristics details.
type TableHeuristicsDetails = TableHeuristicDetailsRow & {
  error?: Error;
};
type TableHeuristicDetailsRow = {
  stats_last_created_at: moment.Moment;
};
// TODO(thomas): currently, this is wrong. Need to properly handle schema name.
//  Table name is schemaName.tableName, need to split and identify or something.
const getTableHeuristicsDetails: TableDetailsQuery<TableHeuristicDetailsRow> = {
  createStmt: (dbName, tableName) => {
    const fullyQualifiedName = Join(
      [new Identifier(dbName), "public", new Identifier(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(
        `SELECT max(created) AS created FROM [SHOW STATISTICS FOR TABLE %1]`,
        [fullyQualifiedName],
      ),
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableHeuristicDetailsRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      resp.heuristics_details.stats_last_created_at =
        txn_result.rows[0].stats_last_created_at;
    }
    if (txn_result.error) {
      resp.schema_details.error = txn_result.error;
    }
  },
};

// Table details stats.
type TableDetailsStats = {
  ranges_data: TableRangesData;
  pebble_data: TablePebbleData;
  index_stats: TableIndexUsageStats;
};
type TablePebbleData = {
  approximate_disk_bytes: number;
};
type TableRangesData = {
  range_count: number;
  live_bytes: number;
  total_bytes: number;
  live_percentage: number;
  node_ids: number[];
  node_count: number;
  replica_count: number;
  error?: Error;
};

// Table span stats.
type TableSpanStatsRow = {
  approximate_disk_bytes: number;
  live_bytes: number;
  total_bytes: number;
  range_count: number;
  live_percentage: number;
};
const getTableSpanStats: TableDetailsQuery<TableSpanStatsRow> = {
  createStmt: (dbName, tableName) => {
    return {
      sql: `WITH 
             dbId AS (SELECT crdb_internal.get_database_id($1) as database_id),
             tableId AS (SELECT $2::regclass::int as table_id)
            SELECT
              range_count,
              approximate_disk_bytes,
              live_bytes,
              total_bytes,
              live_percentage
            FROM crdb_internal.tenant_span_stats(
                (SELECT database_id FROM dbId), 
                (SELECT table_id FROM tableId)
            )`,
      arguments: [dbName, tableName],
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableSpanStatsRow>,
    resp: TableDetailsResponse,
  ) => {
    // If rows are not null or empty...
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      resp.stats.pebble_data.approximate_disk_bytes =
        txn_result.rows[0].approximate_disk_bytes;
      resp.stats.ranges_data.range_count = txn_result.rows[0].range_count;
      resp.stats.ranges_data.live_bytes = txn_result.rows[0].live_bytes;
      resp.stats.ranges_data.total_bytes = txn_result.rows[0].total_bytes;
      resp.stats.ranges_data.live_percentage = txn_result.rows[0].live_bytes;
    } else {
      txn_result.error = new Error(
        "getTableSpanStats: unexpected empty results",
      );
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Table replicas.
type TableReplicasRow = {
  replicas: number[];
};
const getTableReplicas: TableDetailsQuery<TableReplicasRow> = {
  createStmt: (dbName, tableName) => {
    return {
      sql: Format(
        `SELECT
            r.replicas,
          FROM crdb_internal.tables as t
          JOIN %1.crdb_internal.table_spans as s ON s.descriptor_id = t.table_id
          JOIN crdb_internal.ranges_no_leases as r ON s.start_key < r.end_key AND s.end_key > r.start_key
          WHERE t.database_name = $1 AND t.name = $2`,
        [new Identifier(dbName)],
      ),
      arguments: [dbName, tableName],
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<TableReplicasRow>,
    resp: TableDetailsResponse,
  ) => {
    // Build set of unique replicas for this database.
    const replicas = new Set<number>();
    // If rows are not null or empty...
    if (!(!txn_result.rows || txn_result.rows?.length === 0)) {
      txn_result.rows.forEach(row => {
        row.replicas.forEach(replicas.add, replicas);
      });
      // Currently we use replica ids as node ids.
      resp.stats.ranges_data.replica_count = replicas.size;
      resp.stats.ranges_data.node_count = replicas.size;
      resp.stats.ranges_data.node_ids = Array.from(replicas.values());
    }
    if (txn_result.error) {
      resp.stats.ranges_data.error = txn_result.error;
    }
  },
};

// Table index usage stats.
type TableIndexUsageStats = {
  has_index_recommendations: boolean;
  error?: Error;
};
const getTableIndexUsageStats: TableDetailsQuery<IndexUsageStatistic> = {
  createStmt: (dbName, tableName) => {
    return {
      sql: Format(
        `WITH cs AS (
          SELECT value 
              FROM crdb_internal.cluster_settings 
          WHERE variable = 'sql.index_recommendation.drop_unused_duration'
          )
          SELECT * FROM (SELECT
                  ti.created_at,
                  us.last_read,
                  us.total_reads,
                  cs.value as unused_threshold,
                  cs.value::interval as interval_threshold,
                  now() - COALESCE(us.last_read AT TIME ZONE 'UTC', COALESCE(ti.created_at, '0001-01-01')) as unused_interval
                  FROM %1.crdb_internal.index_usage_statistics AS us
                  JOIN %1.crdb_internal.table_indexes AS ti ON (
                      us.index_id = ti.index_id AND 
                      us.table_id = ti.descriptor_id AND 
                      ti.index_type = 'secondary' AND 
                      ti.descriptor_name = $2
                  )
                  CROSS JOIN cs
                 WHERE $1 != 'system')
               WHERE unused_interval > interval_threshold
               ORDER BY total_reads DESC;`,
        [new Identifier(dbName)],
      ),
      arguments: [dbName, tableName],
    };
  },
  addToTableDetail: (
    txn_result: SqlTxnResult<IndexUsageStatistic>,
    resp: TableDetailsResponse,
  ) => {
    resp.stats.index_stats.has_index_recommendations = txn_result.rows?.some(
      row => recommendDropUnusedIndex(row),
    );
    if (txn_result.error) {
      resp.stats.index_stats.error = txn_result.error;
    }
  },
};

type TableDetailsQuery<RowType> = {
  createStmt: (dbName: string, tableName: string) => SqlStatement;
  addToTableDetail: (
    response: SqlTxnResult<RowType>,
    tableDetail: TableDetailsResponse,
  ) => void;
};

type TableDetailsRow =
  | TableIdRow
  | TableGrantsRow
  | TableSchemaDetailsRow
  | TableCreateStatementRow
  | TableZoneConfigStatementRow
  | TableHeuristicDetailsRow
  | TableSpanStatsRow
  | IndexUsageStatistic
  | TableZoneConfigRow
  | TableReplicasRow;

const tableDetailQueries: TableDetailsQuery<TableDetailsRow>[] = [
  getTableId,
  getTableGrants,
  getTableSchemaDetails,
  getTableCreateStatement,
  getTableZoneConfigStmt,
  getTableHeuristicsDetails,
  getTableSpanStats,
  getTableIndexUsageStats,
  getTableZoneConfig,
  getTableReplicas,
];

export function createTableDetailsReq(
  dbName: string,
  tableName: string,
): SqlExecutionRequest {
  return {
    execute: true,
    statements: tableDetailQueries.map(query =>
      query.createStmt(dbName, tableName),
    ),
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
    database: dbName,
  };
}

export async function getTableDetails(
  databaseName: string,
  tableName: string,
  timeout?: moment.Duration,
): Promise<SqlApiResponse<TableDetailsResponse>> {
  return withTimeout(fetchTableDetails(databaseName, tableName), timeout);
}

async function fetchTableDetails(
  databaseName: string,
  tableName: string,
): Promise<SqlApiResponse<TableDetailsResponse>> {
  const detailsResponse: TableDetailsResponse = newTableDetailsResponse();
  const req: SqlExecutionRequest = createTableDetailsReq(
    databaseName,
    tableName,
  );
  const resp = await executeInternalSql<TableDetailsRow>(req);
  resp.execution.txn_results.forEach(txn_result => {
    if (txn_result.rows) {
      const query: TableDetailsQuery<TableDetailsRow> =
        tableDetailQueries[txn_result.statement - 1];
      query.addToTableDetail(txn_result, detailsResponse);
    }
  });
  if (resp.error) {
    detailsResponse.error = resp.error;
  }
  return formatApiResult<TableDetailsResponse>(
    detailsResponse,
    detailsResponse.error,
    "retrieving table details information",
  );
}
