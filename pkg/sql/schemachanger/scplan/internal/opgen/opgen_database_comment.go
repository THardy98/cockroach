// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opgen

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
)

func init() {
	opRegistry.register((*scpb.DatabaseComment)(nil),
		toPublic(
			scpb.Status_ABSENT,
			to(scpb.Status_PUBLIC,
				emit(func(this *scpb.DatabaseComment) *scop.UpsertDatabaseComment {
					return &scop.UpsertDatabaseComment{
						DatabaseID: this.DatabaseID,
						Comment:    this.Comment,
					}
				}),
				emit(func(this *scpb.DatabaseComment, md *targetsWithElementMap) *scop.LogEvent {
					return newLogEventOp(this, md)
				}),
			),
		),
		toAbsent(
			scpb.Status_PUBLIC,
			to(scpb.Status_ABSENT,
				emit(func(this *scpb.DatabaseComment) *scop.RemoveDatabaseComment {
					return &scop.RemoveDatabaseComment{
						DatabaseID: this.DatabaseID,
					}
				}),
				emit(func(this *scpb.DatabaseComment, md *targetsWithElementMap) *scop.LogEvent {
					return newLogEventOp(this, md)
				}),
			),
		),
	)
}
