// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import styles from "src/statementsPage/statementsPage.module.scss";
import sortableTableStyles from "src/sortedtable/sortedtable.module.scss";
import {ISortedTablePagination, SortSetting} from "../../sortedtable";
import classNames from "classnames/bind";
import {PageConfig, PageConfigItem} from "../../pageConfig";
import {Loading} from "../../loading";
import {useEffect, useState} from "react";
import {useHistory} from "react-router-dom";
import {InsightsSortedTable, makeInsightsColumns, InsightRecommendation} from "../../insightsTable/insightsTable";
import {calculateActiveFilters, Filter, getFullFiltersAsStringRecord} from "../../queryFilter";
import {queryByName, syncHistory} from "../../util";
import {getTableSortFromURL} from "../../sortedtable/getTableSortFromURL";
import {WorkloadInsightsError} from "../workloadInsights/workloadInsightsTable";
import {defaultSchemaInsightFilters, SchemaInsightFilters} from "../utils";
import {getSchemaInsightFiltersFromURL} from "../../queryFilter/utils";
import {TableStatistics} from "../../tableStatistics";
import {getSchemaInsights} from "../../api/schemaInsightsApi";
const cx = classNames.bind(styles);
const sortableTableCx = classNames.bind(sortableTableStyles);

const mockSchemaInsights: InsightRecommendation[] = [
  {
    type: "DROP_INDEX",
    database: "test_db",
    table: "test_table",
    index_id: 2,
    query: "this is a mock query",
    execution: null
  },
  {
    type: "CREATE_INDEX",
    database: "test_db",
    table: "test_table",
    index_id: 1,
    query: "this is a mock query",
    execution: {
      statement: "this is a mock stmt",
      summary: "this is a mock summary",
      fingerprintID: "mock fingerprintID",
      implicit: true,
    }

  },
  {
    type: "DROP_INDEX",
    database: "test_db",
    table: "test_table",
    index_id: 3,
    query: "this is a mock query",
    execution: null

  }
];

export type SchemaInsightsViewStateProps = {
  // transactions: InsightEventsResponse;
  // transactionsError: Error | null;
  filters: SchemaInsightFilters;
  sortSetting: SortSetting;
  // internalAppNamePrefix: string;
};

export type SchemaInsightsViewDispatchProps = {
  onFiltersChange: (filters: SchemaInsightFilters) => void;
  onSortChange: (ss: SortSetting) => void;
  // refreshTransactionInsights: () => void;
};

export type SchemaInsightsViewProps = SchemaInsightsViewStateProps &
  SchemaInsightsViewDispatchProps;

export const SchemaInsightsView: React.FC<SchemaInsightsViewProps> = ({filters, sortSetting, onSortChange, onFiltersChange}: SchemaInsightsViewProps) => {
  const [pagination, setPagination] = useState<ISortedTablePagination>({
    current: 1,
    pageSize: 10,
  });
  const history = useHistory();

  useEffect(() => {
    // We use this effect to sync settings defined on the URL (sort, filters),
    // with the redux store. The only time we do this is when the user navigates
    // to the page directly via the URL and specifies settings in the query string.
    // Note that the desired behaviour is currently that the user is unable to
    // clear filters via the URL, and must do so with page controls.
    const sortSettingURL = getTableSortFromURL(history.location);
    const filtersFromURL = getSchemaInsightFiltersFromURL(history.location);

    if (sortSettingURL) {
      onSortChange(sortSettingURL);
    }
    if (filtersFromURL) {
      onFiltersChange(filtersFromURL);
    }
  }, [history, onSortChange]);

  useEffect(() => {
    // This effect runs when the filters or sort settings received from
    // redux changes and syncs the URL params with redux.
    syncHistory(
      {
        ascending: sortSetting.ascending.toString(),
        columnTitle: sortSetting.columnTitle,
        ...getFullFiltersAsStringRecord(filters),
      },
      history,
    );
  }, [
    history,
    filters,
    sortSetting.ascending,
    sortSetting.columnTitle,
  ]);

  const onChangePage = (current: number): void => {
    setPagination({
      current: current,
      pageSize: 10,
    });
  };

  const resetPagination = () => {
    setPagination({
      current: 1,
      pageSize: 10,
    });
  };

  const onChangeSortSetting = (ss: SortSetting): void => {
    onSortChange(ss);
    resetPagination();
  };

  const onSubmitFilters = (selectedFilters: SchemaInsightFilters) => {
    onFiltersChange(selectedFilters);
    resetPagination();
  };

  const clearFilters = () => onSubmitFilters(defaultSchemaInsightFilters);
  const countActiveFilters = calculateActiveFilters(filters);

  // TODO(thomas): get actual insights & apps
  // const apps = getAppsFromTransactionInsights(
  //   transactionInsights,
  //   internalAppNamePrefix,
  // );
  // const filteredTransactions = filterTransactionInsights(
  //   transactionInsights,
  //   filters,
  //   internalAppNamePrefix,
  //   search,
  // );

  const columns = makeInsightsColumns();
  const data = mockSchemaInsights;

  console.log("FILTERS", filters);
  console.log("Schema insights api results", getSchemaInsights());

  return (
    <div className={cx("root")}>
      <PageConfig>
        <PageConfigItem>
          <Filter
            activeFilters={countActiveFilters}
            onSubmitFilters={onSubmitFilters}
            // TODO(thomas): fix?
            appNames={[]}
            filters={filters}
          />
        </PageConfigItem>
      </PageConfig>
      <div className={cx("table-area")}>
        {/*<Loading*/}
        {/*  loading={transactions == null}*/}
        {/*  page="transaction insights"*/}
        {/*  error={transactionsError}*/}
        {/*  renderError={() =>*/}
        {/*    WorkloadInsightsError({*/}
        {/*      execType: "transaction insights",*/}
        {/*    })*/}
        {/*  }*/}
        {/*>*/}
          <div>
            <section className={sortableTableCx("cl-table-container")}>
              <div>
                <TableStatistics
                  pagination={pagination}
                  // search={search}
                  totalCount={mockSchemaInsights.length}
                  arrayItemName="transaction insights"
                  activeFilters={countActiveFilters}
                  onClearFilters={clearFilters}
                />
              </div>
              <InsightsSortedTable
                columns={columns}
                data={data}
                sortSetting={sortSetting}
                onChangeSortSetting={onChangeSortSetting}
              />
            </section>
            {/*<Pagination*/}
            {/*  pageSize={pagination.pageSize}*/}
            {/*  current={pagination.current}*/}
            {/*  total={filteredTransactions?.length}*/}
            {/*  onChange={onChangePage}*/}
            {/*/>*/}
          </div>
        {/*</Loading>*/}
      </div>
    </div>
  );
}
