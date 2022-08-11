import {
  SchemaInsightFilters,
  SchemaInsightsView,
  SchemaInsightsViewDispatchProps,
  SchemaInsightsViewStateProps,
  SortSetting,
} from "@cockroachlabs/cluster-ui";
import {AdminUIState} from "src/redux/state";
import {RouteComponentProps, withRouter} from "react-router-dom";
import {
  schemaFiltersLocalSetting,
   schemaInsightsSortLocalSetting,
 } from "src/views/insights/redux";
import {connect} from "react-redux";

const mapStateToProps = (
  state: AdminUIState,
  _props: RouteComponentProps,
): SchemaInsightsViewStateProps => ({
  // transactions: selectInsights(state),
  // transactionsError: state.cachedData?.insights.lastError,
  filters: schemaFiltersLocalSetting.selector(state),
  sortSetting: schemaInsightsSortLocalSetting.selector(state),
  // internalAppNamePrefix: selectAppName(state),
});

const mapDispatchToProps = {
  onFiltersChange: (filters: SchemaInsightFilters) =>
     schemaFiltersLocalSetting.set(filters),
   onSortChange: (ss: SortSetting) => schemaInsightsSortLocalSetting.set(ss),
   // refreshTransactionInsights: refreshInsights,
 };

const SchemaInsightsPageConnected = withRouter(
  connect<
     SchemaInsightsViewStateProps,
     SchemaInsightsViewDispatchProps,
     RouteComponentProps
     >(
       mapStateToProps,
        mapDispatchToProps,
      )(SchemaInsightsView),
);

export default SchemaInsightsPageConnected;
