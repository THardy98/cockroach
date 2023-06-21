// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import { Tooltip } from "antd";
import "antd/lib/tooltip/style";

export function checkInfoAvailable(
  requestError: Error,
  queryError: Error,
  cell: React.ReactNode,
): React.ReactNode {
  // If we encountered a request error when fetching details, all cells in this row are unavailable.
  if (requestError) {
    return "(unavailable)";
  }
  // If we encounter a query-level error (i.e. error for the query populating
  // this particular cell), this cell is unavailable.
  if (queryError || cell == null) {
    return (
      <Tooltip
        placement="bottom"
        title={
          queryError
            ? `${queryError.message}`
            : "query returned an empty result"
        }
      >
        (unavailable)
      </Tooltip>
    );
  }
  return cell;
}
