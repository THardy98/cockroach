// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { AppState } from "../reducers";

export const selectAutomaticStatsCollectionEnabled = (
  state: AppState,
): boolean => {
  const settings = state.adminUI?.clusterSettings.data?.key_values;
  if (!settings) {
    return false;
  }
  return settings["sql.stats.automatic_collection.enabled"].value === "true";
};
