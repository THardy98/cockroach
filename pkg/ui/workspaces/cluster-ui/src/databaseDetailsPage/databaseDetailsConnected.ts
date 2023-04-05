// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps } from "react-router";
import { databaseNameAttr } from "src/util/constants";
import { getMatchParamByName } from "src/util/query";
import { AppState } from "../store";
import { Dispatch } from "redux";
import { actions as databaseDetailsActions } from "../store/databaseDetails";
import {
  actions as localStorageActions,
  LocalStorageKeys,
} from "../store/localStorage";
import { actions as tableDetailsActions } from "../store/databaseTableDetails";
import {
  DatabaseDetailsPageActions,
  DatabaseDetailsPageData,
  ViewMode,
} from "./databaseDetailsPage";
import { actions as analyticsActions } from "../store/analytics";
import { Filters } from "../queryFilter";
import { nodeRegionsByIDSelector } from "../store/nodes";
import { selectIsTenant } from "../store/uiConfig";
import {
  selectDatabaseDetails,
  selectDatabaseDetailsGrantsSortSetting,
  selectDatabaseDetailsTablesFiltersSetting,
  selectDatabaseDetailsTablesSearchSetting,
  selectDatabaseDetailsTablesSortSetting,
  selectDatabaseDetailsViewModeSetting,
} from "../store/databaseDetails/databaseDetails.selectors";
import { combineLoadingErrors } from "../util";
import { selectTables } from "../selectors/databasesCommon.selectors";

export const mapStateToProps = (
  state: AppState,
  props: RouteComponentProps,
): DatabaseDetailsPageData => {
  const database = getMatchParamByName(props.match, databaseNameAttr);
  const databaseDetails = selectDatabaseDetails(state);
  const dbTables =
    databaseDetails[database]?.data?.results.tablesResp.tables || [];
  const nodeRegions = nodeRegionsByIDSelector(state);
  const isTenant = selectIsTenant(state);
  return {
    loading: !!databaseDetails[database]?.inFlight,
    loaded: !!databaseDetails[database]?.valid,
    lastError: combineLoadingErrors(
      databaseDetails[database]?.lastError,
      databaseDetails[database]?.data?.maxSizeReached,
      null,
    ),
    name: database,
    // Default setting this to true. On db-console we only display this when
    // we have >1 node, but I'm not sure why.
    showNodeRegionsColumn: true,
    viewMode: selectDatabaseDetailsViewModeSetting(state),
    sortSettingTables: selectDatabaseDetailsTablesSortSetting(state),
    sortSettingGrants: selectDatabaseDetailsGrantsSortSetting(state),
    filters: selectDatabaseDetailsTablesFiltersSetting(state),
    search: selectDatabaseDetailsTablesSearchSetting(state),
    nodeRegions,
    isTenant,
    tables: selectTables(
      database,
      dbTables,
      state.adminUI?.tableDetails,
      nodeRegions,
      isTenant,
    ),
  };
};

export const mapDispatchToProps = (
  dispatch: Dispatch,
): DatabaseDetailsPageActions => ({
  refreshDatabaseDetails: (database: string) => {
    dispatch(databaseDetailsActions.refresh(database));
  },
  refreshTableDetails: (database: string, table: string) => {
    dispatch(tableDetailsActions.refresh({ database, table }));
  },
  onViewModeChange: (viewMode: ViewMode) => {
    dispatch(
      analyticsActions.track({
        name: "View Mode Clicked",
        page: "Database Details",
        value: ViewMode[viewMode],
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_VIEW_MODE,
        value: viewMode,
      }),
    );
  },
  onSortingTablesChange: (columnName: string, ascending: boolean) => {
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Database Details",
        tableName: "Database Details",
        columnName,
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SORT,
        value: { columnTitle: columnName, ascending: ascending },
      }),
    );
  },
  onSortingGrantsChange: (columnName: string, ascending: boolean) => {
    dispatch(
      analyticsActions.track({
        name: "Column Sorted",
        page: "Database Details",
        tableName: "Database Details",
        columnName,
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_GRANTS_PAGE_SORT,
        value: { columnTitle: columnName, ascending: ascending },
      }),
    );
  },
  onSearchComplete: (query: string) => {
    dispatch(
      analyticsActions.track({
        name: "Keyword Searched",
        page: "Database Details",
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_TABLES_PAGE_SEARCH,
        value: query,
      }),
    );
  },
  onFilterChange: (filters: Filters) => {
    dispatch(
      analyticsActions.track({
        name: "Filter Clicked",
        page: "Database Details",
        filterName: "filters",
        value: filters.toString(),
      }),
    );
    dispatch(
      localStorageActions.update({
        key: LocalStorageKeys.DB_DETAILS_TABLES_PAGE_FILTERS,
        value: filters,
      }),
    );
  },
});
