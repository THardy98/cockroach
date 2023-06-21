// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React, { useContext } from "react";
import {
  EncodeDatabaseTableUri,
  EncodeDatabaseUri,
  EncodeUriName,
  getMatchParamByName,
  schemaNameAttr,
} from "../util";
import { Link } from "react-router-dom";
import { DatabaseIcon } from "../icon/databaseIcon";
import {
  DatabaseDetailsPageDataTable,
  DatabaseDetailsPageProps,
  ViewMode,
} from "./databaseDetailsPage";
import classNames from "classnames/bind";
import styles from "./databaseDetailsPage.module.scss";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";
import { Caution } from "@cockroachlabs/icons";
import * as format from "../util/format";
import { Breadcrumbs } from "../breadcrumbs";
import { CaretRight } from "../icon/caretRight";
import { CockroachCloudContext } from "../contexts";
import {
  isMaxSizeError,
  isPrivilegeError,
  SqlExecutionErrorMessage,
  TableSchemaDetailsRow,
  TableSpanStatsRow,
} from "../api";

const cx = classNames.bind(styles);

export const TableNameCell = ({
  table,
  dbDetails,
}: {
  table: DatabaseDetailsPageDataTable;
  dbDetails: DatabaseDetailsPageProps;
}): JSX.Element => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  let linkURL = "";
  if (isCockroachCloud) {
    linkURL = `${location.pathname}/${EncodeUriName(
      getMatchParamByName(dbDetails.match, schemaNameAttr),
    )}/${EncodeUriName(table.name)}`;
    if (dbDetails.viewMode === ViewMode.Grants) {
      linkURL += `?viewMode=${ViewMode.Grants}`;
    }
  } else {
    linkURL = EncodeDatabaseTableUri(dbDetails.name, table.name);
    if (dbDetails.viewMode === ViewMode.Grants) {
      linkURL += `?tab=grants`;
    }
  }

  let icon = <DatabaseIcon className={cx("icon--s", "icon--primary")} />;
  if (table.requestError || table.queryError) {
    icon = (
      <Tooltip
        placement="bottom"
        title={getTooltipErrorMessage(
          table.requestError,
          table.queryError,
          table.name,
        )}
      >
        <Caution className={cx("icon--s", "icon--warning")} />
      </Tooltip>
    );
  }
  return (
    <Link to={linkURL} className={cx("icon__container")}>
      {icon}
      {table.name}
    </Link>
  );
};

export const IndexRecWithIconCell = ({
  details,
}: {
  details: TableSchemaDetailsRow;
}): JSX.Element => {
  return (
    <div className={cx("icon__container")}>
      <Tooltip
        placement="bottom"
        title="This table has index recommendations. Click the table name to see more details."
      >
        <Caution className={cx("icon--s", "icon--warning")} />
      </Tooltip>
      {details.indexes.length}
    </div>
  );
};

export const MVCCInfoCell = ({
  details,
}: {
  details: TableSpanStatsRow;
}): JSX.Element => {
  return (
    <>
      <p className={cx("multiple-lines-info")}>
        {format.Percentage(details.live_percentage, 1, 1)}
      </p>
      <p className={cx("multiple-lines-info")}>
        <span className={cx("bold")}>{format.Bytes(details.live_bytes)}</span>{" "}
        live data /{" "}
        <span className={cx("bold")}>{format.Bytes(details.total_bytes)}</span>
        {" total"}
      </p>
    </>
  );
};

export const DbDetailsBreadcrumbs = ({ dbName }: { dbName: string }) => {
  const isCockroachCloud = useContext(CockroachCloudContext);
  return (
    <Breadcrumbs
      items={[
        { link: "/databases", name: "Databases" },
        {
          link: isCockroachCloud
            ? `/databases/${EncodeUriName(dbName)}`
            : EncodeDatabaseUri(dbName),
          name: "Tables",
        },
      ]}
      divider={<CaretRight className={cx("icon--xxs", "icon--primary")} />}
    />
  );
};

const getTooltipErrorMessage = (
  requestError: Error,
  queryError: SqlExecutionErrorMessage,
  tableName: string,
): string => {
  if (requestError) {
    return `Encountered an error fetching table statistics. Table statistics are unavailable for table ${tableName}`;
  }
  if (queryError) {
    if (isPrivilegeError(queryError.code)) {
      return `Not all table statistics available, user has insufficient privileges for some statistics`;
    }
    if (isMaxSizeError(queryError.message)) {
      return `Not all table statistics available, maximum size of table statistics was reached in the console`;
    }
    return `Unexpected query error, not all table statistics available`;
  }
};
