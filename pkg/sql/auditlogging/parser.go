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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings/rulebasedscanner"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"strings"
)

// parse parses the provided audit logging configuration.
func parse(input string) (*AuditConfig, error) {
	tokens, err := rulebasedscanner.Tokenize(input)
	if err != nil {
		return nil, err
	}

	config := EmptyAuditConfig()
	config.Settings = make([]AuditSetting, len(tokens.Lines))
	for i, line := range tokens.Lines {
		setting, err := parseAuditSetting(line)
		if err != nil {
			return nil, errors.Wrapf(
				pgerror.WithCandidateCode(err, pgcode.ConfigFile),
				"line %d", tokens.Linenos[i])
		}
		if _, exists := config.SettingsRoleLookup[setting.Role]; exists {
			return nil, errors.Newf("duplicate role listed: %v", setting.Role)
		}
		config.SettingsRoleLookup[setting.Role] = i
		config.Settings[i] = setting
		if setting.Role.Normalized() == AllUserRole {
			config.AllRoleAuditSettingIdx = i
		}
	}
	return config, nil
}

func parseAuditSetting(inputLine rulebasedscanner.Line) (setting AuditSetting, err error) {
	fieldIdx := 0
	setting.Input = inputLine.Input
	line := inputLine.Tokens

	// Read the user/Role type.
	if len(line[fieldIdx]) > 1 {
		return setting, errors.WithHint(
			errors.New("multiple values specified for role"),
			"Specify exactly one role type per line.")
	}
	// Note we do not do any validation to ensure the input Role exists as an actual Role. This allows for
	// input roles to be arbitrary string values.
	setting.Role, err = username.MakeSQLUsernameFromUserInput(line[fieldIdx][0].Value, username.PurposeValidation)
	if err != nil {
		return setting, err
	}
	err = parseRole(setting.Role)
	if err != nil {
		return setting, err
	}
	// parse statement types
	fieldIdx++
	if fieldIdx >= len(line) {
		return setting, errors.New("end-of-line before statement types specification")
	}
	setting.StatementTypes, err = parseStatementTypes(line[fieldIdx])
	return setting, err
}

func parseRole(role username.SQLUsername) error {
	// Cannot use reserved role names.
	if role.IsPublicRole() || role.IsNoneRole() {
		return errors.Newf("cannot use reserved role name: '%s'", role.Normalized())
	}
	// Cannot use node user.
	if role.IsNodeUser() {
		return errors.Newf("cannot use reserved username: '%s'", role.Normalized())
	}
	// Cannot use 'pg_' prefix, reserved.
	if strings.HasPrefix(role.Normalized(), "pg_") {
		return errors.Newf("cannot use 'pg_' prefix in role name: '%s'", role.Normalized())
	}
	// Cannot use 'crdb_internal_' prefix, reserved.
	if strings.HasPrefix(role.Normalized(), "crdb_internal_") {
		return errors.Newf("cannot use 'crdb_internal_' prefix in role name: '%s'", role.Normalized())
	}
	return nil
}

// parseStatementTypes parses the statement type field.
func parseStatementTypes(stmtTypes []rulebasedscanner.String) (types []tree.StatementType, err error) {
	for _, stmtType := range stmtTypes {
		val := strings.ToUpper(stmtType.Value)
		switch val {
		case "DDL":
			types = append(types, tree.TypeDDL)
		case "DML":
			types = append(types, tree.TypeDML)
		case "DCL":
			types = append(types, tree.TypeDCL)
		case "TCL":
			types = append(types, tree.TypeTCL)
		case "ALL":
			if len(types) > 0 {
				return types, errors.Newf(`redundant statement types with "ALL"`)
			}
			types = append(types, AuditAllStatementsConst)
		case "NONE":
			if len(types) > 0 {
				return types, errors.Newf(`redundant statement types with "NONE"`)
			}
			types = append(types, AuditNoneStatementConst)
		default:
			return types, errors.WithHint(errors.Newf(
				`unknown statement type: %q (valid types include: "DDL", "DML", "DCL", "TCL", "ALL", "NONE")`, stmtType.Value,
			), "Statement types are normalized (i.e. Ddl, ddl are valid inputs for DDL)")
		}
	}
	return types, nil
}
