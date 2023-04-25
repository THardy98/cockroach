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
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging"
	"github.com/cockroachdb/cockroach/pkg/sql/auditlogging/auditevents"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func (p *planner) maybeAuditSensitiveTableAccessEvent(
	privilegeObject privilege.Object,
	priv privilege.Kind,
) {
	if p.shouldNotAuditInternal() {
		return
	}
	// Check if we can add this event.
	tableDesc, ok := privilegeObject.(catalog.TableDescriptor)
	if !ok || tableDesc.GetAuditMode() == descpb.TableDescriptor_DISABLED {
		return
	}
	writing := false
	switch priv {
	case privilege.INSERT, privilege.DELETE, privilege.UPDATE:
		writing = true
	}
	p.curPlan.auditEventBuilders = append(p.curPlan.auditEventBuilders,
		&auditevents.SensitiveTableAccessEvent{TableDesc: tableDesc, Writing: writing},
	)
}

func (p *planner) maybeAuditRoleBasedAuditEvent() {
	// Avoid doing audit logging work if we don't have to.
	if p.shouldNotRoleBasedAudit() {
		return
	}
	// If the reduced config is nil, there were no matching audit settings. Return early.
	if p.reducedAuditConfig == nil {
		return
	}

	stmtType := p.stmt.AST.StatementType()
	if p.reducedAuditConfig.CheckMatchingStatementType(stmtType) {
		sessionConnDetails := p.execCfg.SessionInitCache.ReadSessionConnCache(p.User())
		p.curPlan.auditEventBuilders = append(p.curPlan.auditEventBuilders,
			&auditevents.RoleBasedAuditEvent{
				Setting:        p.reducedAuditConfig,
				StatementType:  stmtType.String(),
				DatabaseName:   p.CurrentDatabase(),
				ServerAddress:  sessionConnDetails.ServerAddr,
				RemoteAddress:  sessionConnDetails.RemoteAddr,
				ConnectionType: sessionConnDetails.ConnMethod,
			},
		)
	}
}

func (p *planner) initializeReducedAuditConfig(ctx context.Context) {
	// Avoid doing audit config setup if we don't have to.
	if p.shouldNotRoleBasedAudit() {
		return
	}

	err := p.execCfg.InternalDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		userRoles, err := MemberOfWithAdminOption(ctx, p.execCfg, txn, p.User())
		if err != nil {
			return err
		}
		p.execCfg.SessionInitCache.AuditConfig.Lock()
		defer p.execCfg.SessionInitCache.AuditConfig.Unlock()
		p.reducedAuditConfig = p.execCfg.SessionInitCache.AuditConfig.Config.GetMatchingAuditSetting(userRoles)
		return nil
	})
	if err != nil {
		fmt.Println("RoleBasedAuditEvent: error getting user role memberships", err)
		log.Errorf(ctx, "RoleBasedAuditEvent: error getting user role memberships: %v", err)
		return
	}
}

// shouldNotRoleBasedAudit checks if we should do any auditing work for RoleBasedAuditEvents.
func (p *planner) shouldNotRoleBasedAudit() bool {
	// Do not do audit work if the cluster setting is empty.
	return auditlogging.UserAuditLogConfig.Get(&p.execCfg.Settings.SV) == "" || p.shouldNotAuditInternal()
}

func (p *planner) shouldNotAuditInternal() bool {
	// Do not emit audit events for reserved users/roles. This does not omit the root user.
	// Do not emit audit events for internal usage (internal planner/internal executor).
	return p.User().IsReserved() || p.isInternalPlanner || p.extendedEvalCtx.ExecType == executorTypeInternal
}
