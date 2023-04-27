// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtestutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRoleBasedAuditLogging(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := log.ScopeWithoutShowLogs(t)
	defer sc.Close(t)

	cleanup := logtestutils.InstallLogFileSink(sc, t, logpb.Channel_SENSITIVE_ACCESS)
	defer cleanup()

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	db := sqlutils.MakeSQLRunner(sqlDB)
	defer s.Stopper().Stop(ctx)

	// Dummy table/user used by tests.
	db.Exec(t, `CREATE TABLE u(x int)`)
	db.Exec(t, `CREATE USER test_user`)

	t.Run("testSingleRoleAuditLogging", func(t *testing.T) {
		testSingleRoleAuditLogging(t, ctx, sqlDB)
	})
	// Reset audit config between tests.
	db.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = ''`)
	t.Run("testMultiRoleAuditLogging", func(t *testing.T) {
		testMultiRoleAuditLogging(t, ctx, sqlDB)
	})
}

func testSingleRoleAuditLogging(t *testing.T, ctx context.Context, sqlDB *sql.DB) {
	setupConn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	setupDb := sqlutils.MakeSQLRunner(setupConn)

	allStmtTypesRole := "all_stmt_types"
	someStmtTypeRole := "some_stmt_types"

	setupDb.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", allStmtTypesRole))
	setupDb.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", someStmtTypeRole))

	setupDb.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		all_stmt_types ALL
		some_stmt_types DDL,DML
	'`)

	// Queries for all statement types
	testQueries := []string{
		// DDL statement,
		`ALTER TABLE u RENAME COLUMN x to x`,
		// DCL statement
		`GRANT SELECT ON TABLE u TO test_user`,
		// DML statement
		`SELECT * FROM u`,
	}
	testData := []struct {
		name            string
		role            string
		queries         []string
		expectedNumLogs int
	}{
		{
			name:            "test-some-stmt-types",
			role:            someStmtTypeRole,
			queries:         testQueries,
			expectedNumLogs: 2,
		},
		{
			name:            "test-all-stmt-types",
			role:            allStmtTypesRole,
			queries:         testQueries,
			expectedNumLogs: 3,
		},
	}

	for _, td := range testData {
		// Grant the audit role
		// Note that this query will cause the reduced audit config to initialize.
		setupDb.Exec(t, fmt.Sprintf("GRANT %s to root", td.role))
		// The reduced audit config is initialized at the first attempt to add an audit event and never
		// changed from there. As such, for changes to the cluster setting config or role membership
		// to reflect on the reduced audit config (after its creation), we need to open a separate session/connection
		// for the reduced audit config to be re-created in the new session (thereby having the config/role changes).
		queryConn, err := sqlDB.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		queryDb := sqlutils.MakeSQLRunner(queryConn)
		for idx := range td.queries {
			queryDb.Exec(t, td.queries[idx])
		}
		// Revoke the audit role.
		setupDb.Exec(t, fmt.Sprintf("REVOKE %s from root", td.role))
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		0,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"role_based_audit_event"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	roleToLogs := make(map[string]int)
	for _, entry := range entries {
		for _, td := range testData {
			if strings.Contains(entry.Message, td.role) {
				roleToLogs[td.role]++
			}
		}
	}
	for _, td := range testData {
		numLogs, exists := roleToLogs[td.role]
		if !exists && td.expectedNumLogs != 0 {
			t.Errorf("found no entries for role: %s", td.role)
		}
		require.Equal(t, td.expectedNumLogs, numLogs)
	}
}

// testMultiRoleAuditLogging tests that we emit the expected audit logs when a user belongs to
// multiple roles that correspond to audit settings. We ensure that the expected audit logs
// correspond to *only* the *first matching audit setting* of the user's roles.
func testMultiRoleAuditLogging(t *testing.T, ctx context.Context, sqlDB *sql.DB) {
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	db := sqlutils.MakeSQLRunner(conn)
	startTimestamp := timeutil.Now().Unix()
	roleA := "roleA"
	roleB := "roleB"

	db.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleA))
	db.Exec(t, fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", roleB))
	db.Exec(t, fmt.Sprintf("GRANT %s, %s to root", roleA, roleB))

	db.Exec(t, `SET CLUSTER SETTING sql.log.user_audit = '
		roleA DML
		roleB DDL,DCL
	'`)

	// Queries for all statement types
	testQueries := []string{
		// DDL statement,
		`ALTER TABLE u RENAME COLUMN x to x`,
		// DCL statement
		`GRANT SELECT ON TABLE u TO test_user`,
		// DML statement
		`SELECT * FROM u`,
	}
	testData := struct {
		name               string
		expectedRoleToLogs map[string]int
	}{
		name: "test-multi-role-user",
		expectedRoleToLogs: map[string]int{
			// Expect single log from DML query.
			roleA: 1,
			// Expect no logs from DDL/DCL queries as we match on roleA first.
			roleB: 0,
		},
	}

	// Note that we do not need to create a separate connection here as no cluster setting config
	// role membership changes have occurred since setting the cluster setting's initial value.
	for _, query := range testQueries {
		db.Exec(t, query)
	}

	log.Flush()

	entries, err := log.FetchEntriesFromFiles(
		startTimestamp,
		math.MaxInt64,
		10000,
		regexp.MustCompile(`"EventType":"role_based_audit_event"`),
		log.WithMarkedSensitiveData,
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(entries) == 0 {
		t.Fatal(errors.Newf("no entries found"))
	}

	roleToLogs := make(map[string]int)
	for role, expectedNumLogs := range testData.expectedRoleToLogs {
		for _, entry := range entries {
			// Lowercase the role string as we normalize it for logs.
			if strings.Contains(entry.Message, strings.ToLower(role)) {
				roleToLogs[role]++
			}
		}
		require.Equal(t, expectedNumLogs, roleToLogs[role], "unexpected number of logs for role: '%s'", role)
	}
}
