// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package auditlogging

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/olekukonko/tablewriter"
)

// Auditor is an interface used to check and build different audit events.
type Auditor interface {
	GetQualifiedTableNameByID(ctx context.Context, id int64, requiredType tree.RequiredTableKind) (*tree.TableName, error)
	Txn() *kv.Txn
}

// AuditEventBuilder is the interface used to build different audit events.
type AuditEventBuilder interface {
	BuildAuditEvent(
		context.Context,
		Auditor,
		eventpb.CommonSQLEventDetails,
		eventpb.CommonSQLExecDetails,
	) logpb.EventPayload
}

type AuditConfigLock struct {
	syncutil.RWMutex
	Config *AuditConfig
}

type UnionAuditConfig struct {
	// This is created once at config initialization and never changed.
	Roles []string
	// StmtTypes is a map for easy lookup.
	StmtTypes map[tree.StatementType]interface{}
}

// AuditConfig is a parsed configuration.
type AuditConfig struct {
	// settings are the collection of AuditSettings that make up the AuditConfig.
	settings map[username.SQLUsername]*AuditSetting
}

func EmptyAuditConfig() *AuditConfig {
	return &AuditConfig{
		settings: make(map[username.SQLUsername]*AuditSetting),
	}
}

// GetUnionMatchingSettings checks if any user's roles match any roles configured in the audit config.
// Returns the first matching AuditSetting.
func (c AuditConfig) GetUnionMatchingSettings(
	userRoles map[username.SQLUsername]bool,
) *UnionAuditConfig {
	uc := &UnionAuditConfig{StmtTypes: make(map[tree.StatementType]interface{})}

	if len(userRoles) < len(c.settings) {
		uc.iterateUserRoles(c.settings, userRoles)
	} else {
		uc.iterateAuditSettings(c.settings, userRoles)
	}
	return uc
}

func (uc *UnionAuditConfig) iterateUserRoles(
	settings map[username.SQLUsername]*AuditSetting,
	userRoles map[username.SQLUsername]bool,
) {
	for role := range userRoles {
		setting, exists := settings[role]
		if !exists {
			continue
		}
		uc.updateSetting(setting)
	}
}

func (uc *UnionAuditConfig) iterateAuditSettings(
	settings map[username.SQLUsername]*AuditSetting,
	userRoles map[username.SQLUsername]bool,
) {
	for role, setting := range settings {
		_, exists := userRoles[role]
		if !exists {
			continue
		}
		uc.updateSetting(setting)
	}
}

func (uc *UnionAuditConfig) updateSetting(s *AuditSetting) {
	uc.Roles = append(uc.Roles, s.Role.Normalized())
	for stmtType := range s.StatementTypes {
		uc.StmtTypes[stmtType] = nil
	}
}

func (c AuditConfig) String() string {
	if len(c.settings) == 0 {
		return "# (empty configuration)\n"
	}

	var sb strings.Builder
	sb.WriteString("# Original configuration:\n")
	for _, setting := range c.settings {
		fmt.Fprintf(&sb, "# %s\n", setting.input)
	}
	sb.WriteString("#\n# Interpreted configuration:\n")

	table := tablewriter.NewWriter(&sb)
	table.SetAutoWrapText(false)
	table.SetReflowDuringAutoWrap(false)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetBorder(false)
	table.SetNoWhiteSpace(true)
	table.SetTrimWhiteSpaceAtEOL(true)
	table.SetTablePadding(" ")

	row := []string{"# ROLE", "STATEMENT_TYPE"}
	table.Append(row)
	for _, setting := range c.settings {
		row[0] = setting.Role.Normalized()
		row[1] = strings.Join(writeStatementTypes(setting.StatementTypes), ",")
		table.Append(row)
	}
	table.Render()
	return sb.String()
}

func writeStatementTypes(vals map[tree.StatementType]int) []string {
	if len(vals) == 0 {
		return []string{"NONE"}
	}
	stmtTypes := make([]string, len(vals))
	// Assign statement types in the order they were input.
	for stmtType := range vals {
		stmtTypes[vals[stmtType]] = stmtType.String()
	}
	return stmtTypes
}

// AuditSetting is a single rule in the audit logging configuration.
type AuditSetting struct {
	// input is the original configuration line in the audit logging configuration string.
	input string
	// Role is user/role this audit setting applies for.
	Role username.SQLUsername
	// StatementTypes is a mapping of statement type to the index/order it was input in the config.
	// The order is used so we can print the setting's statement types in the same order it was
	StatementTypes map[tree.StatementType]int
}

func (s AuditSetting) String() string {
	return AuditConfig{settings: map[username.SQLUsername]*AuditSetting{
		s.Role: &s,
	}}.String()
}
