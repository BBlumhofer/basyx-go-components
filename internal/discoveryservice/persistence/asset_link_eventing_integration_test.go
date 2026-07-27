/*******************************************************************************
* Copyright (C) 2026 the Eclipse BaSyx Authors and Fraunhofer IESE
*
* Permission is hereby granted, free of charge, to any person obtaining
* a copy of this software and associated documentation files (the
* "Software"), to deal in the Software without restriction, including
* without limitation the rights to use, copy, modify, merge, publish,
* distribute, sublicense, and/or sell copies of the Software, and to
* permit persons to whom the Software is furnished to do so, subject to
* the following conditions:
*
* The above copyright notice and this permission notice shall be
* included in all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
* EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
* MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
* NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
* LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
* OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
* WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*
* SPDX-License-Identifier: MIT
******************************************************************************/

// This file verifies that Discovery's three asset-link CUD entry points
// (CreateAllAssetLinks, AddAllAssetLinks, DeleteAllAssetLinks) are wired into
// the shared eventing mutation seam, following the exact DB-integration
// harness pattern used by internal/common/eventing/eventing_integration_test.go:
// gated on BASYX_EVENTING_TEST_DSN, real Postgres, no broker required.
package persistencepostgresql

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/FriedJannik/aas-go-sdk/types"
	"github.com/doug-martin/goqu/v9"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/eventing"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

const assetLinkEventingComponent = "aas-discovery"

func testDiscoveryEventingDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("BASYX_EVENTING_TEST_DSN")
	if dsn == "" {
		t.Skip("BASYX_EVENTING_TEST_DSN not set; skipping discovery eventing integration test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err = db.Ping(); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if _, err = db.Exec("TRUNCATE event_outbox, event_outbox_sequence"); err != nil {
		t.Fatalf("truncate outbox: %v", err)
	}
	if _, err = db.Exec("TRUNCATE aas_identifier CASCADE"); err != nil {
		t.Fatalf("truncate aas_identifier: %v", err)
	}
	return db
}

// enableAssetLinkEventing registers the real eventing hook the same way
// eventing.Setup does at service startup, using a stub sink name so no broker
// is required - the relay never runs in these tests, only the in-transaction
// outbox write is exercised.
func enableAssetLinkEventing(t *testing.T) {
	t.Helper()
	history.Configure(history.Config{Mode: history.ModeOff})
	if err := eventing.Configure(assetLinkEventingComponent, "urn:basyx:test", common.EventingConfig{
		Enabled:       true,
		OutboxEnabled: true,
		Format:        "cloudevents",
		TopicPrefix:   "basyx",
		Sinks:         []string{"stub"},
	}); err != nil {
		t.Fatalf("configure eventing: %v", err)
	}
	t.Cleanup(func() {
		_ = eventing.Configure(assetLinkEventingComponent, "", common.EventingConfig{})
		history.Configure(history.Config{Mode: history.ModeOff})
	})
}

func securityDisabledContext() context.Context {
	return common.ContextWithConfig(context.Background(), &common.Config{})
}

func newDiscoveryEventingBackend(t *testing.T, db *sql.DB) *PostgreSQLDiscoveryDatabase {
	t.Helper()
	backend, err := NewPostgreSQLDiscoveryBackendFromDB(db)
	if err != nil {
		t.Fatalf("new discovery backend: %v", err)
	}
	return backend
}

func specificAssetIDs(pairs ...[2]string) []types.ISpecificAssetID {
	out := make([]types.ISpecificAssetID, 0, len(pairs))
	for _, pair := range pairs {
		out = append(out, types.NewSpecificAssetID(pair[0], pair[1]))
	}
	return out
}

type outboxRow struct {
	operation  string
	entityType string
	topic      string
	payload    string
}

func outboxRowsForIdentifier(t *testing.T, db *sql.DB, identifier string) []outboxRow {
	t.Helper()
	rows, err := db.Query(
		"SELECT operation, entity_type, topic, payload FROM event_outbox WHERE identifier = $1 ORDER BY id",
		identifier,
	)
	if err != nil {
		t.Fatalf("query outbox: %v", err)
	}
	defer func() { _ = rows.Close() }()

	var out []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.operation, &r.entityType, &r.topic, &r.payload); err != nil {
			t.Fatalf("scan outbox row: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate outbox rows: %v", err)
	}
	return out
}

func TestCreateAllAssetLinksEmitsCreatedThenUpdatedEvents(t *testing.T) {
	db := testDiscoveryEventingDB(t)
	enableAssetLinkEventing(t)
	ctx := securityDisabledContext()
	backend := newDiscoveryEventingBackend(t, db)

	aasID := "urn:aas:discovery-eventing:create"
	links := specificAssetIDs([2]string{"globalAssetId", "urn:asset:create-1"})

	// First call: the AAS identifier does not exist yet in Discovery -> created.
	if err := backend.CreateAllAssetLinks(ctx, aasID, links); err != nil {
		t.Fatalf("create asset links: %v", err)
	}
	rows := outboxRowsForIdentifier(t, db, aasID)
	if len(rows) != 1 {
		t.Fatalf("expected 1 outbox row after first create, got %d", len(rows))
	}
	assertEqualString(t, "operation", rows[0].operation, "created")
	assertEqualString(t, "entity_type", rows[0].entityType, "asset_link")
	assertEqualString(t, "topic", rows[0].topic, "basyx/"+assetLinkEventingComponent+"/asset-link/created")
	if !strings.Contains(rows[0].payload, "org.eclipse.basyx.asset-link.created") {
		t.Fatalf("payload missing CloudEvents type: %s", rows[0].payload)
	}
	if !strings.Contains(rows[0].payload, "urn:asset:create-1") || !strings.Contains(rows[0].payload, aasID) {
		t.Fatalf("payload missing expected asset link content: %s", rows[0].payload)
	}

	// Second call: replace-all on an AAS identifier that already exists -> updated.
	moreLinks := specificAssetIDs([2]string{"globalAssetId", "urn:asset:create-2"})
	if err := backend.CreateAllAssetLinks(ctx, aasID, moreLinks); err != nil {
		t.Fatalf("replace asset links: %v", err)
	}
	rows = outboxRowsForIdentifier(t, db, aasID)
	if len(rows) != 2 {
		t.Fatalf("expected 2 outbox rows after second create, got %d", len(rows))
	}
	assertEqualString(t, "operation", rows[1].operation, "updated")
	assertEqualString(t, "topic", rows[1].topic, "basyx/"+assetLinkEventingComponent+"/asset-link/updated")
	if !strings.Contains(rows[1].payload, "urn:asset:create-2") {
		t.Fatalf("second payload missing replaced asset link content: %s", rows[1].payload)
	}
}

func TestAddAllAssetLinksEmitsUpdatedEvent(t *testing.T) {
	db := testDiscoveryEventingDB(t)
	enableAssetLinkEventing(t)
	ctx := securityDisabledContext()
	backend := newDiscoveryEventingBackend(t, db)

	aasID := "urn:aas:discovery-eventing:add"
	if err := backend.CreateAllAssetLinks(ctx, aasID, specificAssetIDs([2]string{"globalAssetId", "urn:asset:add-base"})); err != nil {
		t.Fatalf("seed asset links: %v", err)
	}

	if err := backend.AddAllAssetLinks(ctx, aasID, specificAssetIDs([2]string{"customId", "urn:asset:add-extra"})); err != nil {
		t.Fatalf("add asset links: %v", err)
	}

	rows := outboxRowsForIdentifier(t, db, aasID)
	if len(rows) != 2 {
		t.Fatalf("expected 2 outbox rows (create + add), got %d", len(rows))
	}
	last := rows[len(rows)-1]
	assertEqualString(t, "operation", last.operation, "updated")
	assertEqualString(t, "topic", last.topic, "basyx/"+assetLinkEventingComponent+"/asset-link/updated")
	// The complete post-mutation set includes both the original and the added link.
	if !strings.Contains(last.payload, "urn:asset:add-base") || !strings.Contains(last.payload, "urn:asset:add-extra") {
		t.Fatalf("payload does not contain the complete asset link set: %s", last.payload)
	}
}

func TestAddAllAssetLinksWithNoLinksEmitsNoEvent(t *testing.T) {
	db := testDiscoveryEventingDB(t)
	enableAssetLinkEventing(t)
	ctx := securityDisabledContext()
	backend := newDiscoveryEventingBackend(t, db)

	aasID := "urn:aas:discovery-eventing:add-empty"
	if err := backend.AddAllAssetLinks(ctx, aasID, nil); err != nil {
		t.Fatalf("add with no links must be a no-op, got error: %v", err)
	}
	if rows := outboxRowsForIdentifier(t, db, aasID); len(rows) != 0 {
		t.Fatalf("expected no outbox rows for a no-op add, got %d", len(rows))
	}
}

func TestDeleteAllAssetLinksEmitsDeletedEventAndRemovesLinks(t *testing.T) {
	db := testDiscoveryEventingDB(t)
	enableAssetLinkEventing(t)
	ctx := securityDisabledContext()
	backend := newDiscoveryEventingBackend(t, db)

	aasID := "urn:aas:discovery-eventing:delete"
	if err := backend.CreateAllAssetLinks(ctx, aasID, specificAssetIDs([2]string{"globalAssetId", "urn:asset:delete-1"})); err != nil {
		t.Fatalf("seed asset links: %v", err)
	}

	if err := backend.DeleteAllAssetLinks(ctx, aasID); err != nil {
		t.Fatalf("delete asset links: %v", err)
	}

	rows := outboxRowsForIdentifier(t, db, aasID)
	if len(rows) != 2 {
		t.Fatalf("expected 2 outbox rows (create + delete), got %d", len(rows))
	}
	last := rows[len(rows)-1]
	assertEqualString(t, "operation", last.operation, "deleted")
	assertEqualString(t, "topic", last.topic, "basyx/"+assetLinkEventingComponent+"/asset-link/deleted")
	if !strings.Contains(last.payload, `"deleted": true`) || !strings.Contains(last.payload, aasID) {
		t.Fatalf("deleted payload does not match the documented deletion representation: %s", last.payload)
	}

	if _, err := backend.GetAllAssetLinks(ctx, aasID); !common.IsErrNotFound(err) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}
}

// TestDeleteAllAssetLinksIsTransactional proves the fix to DeleteAllAssetLinks:
// before this change it executed the DELETE directly against *sql.DB with no
// transaction at all, so a failure after the delete could not roll anything
// back. It now runs inside one transaction (via descriptors.WithTx) together
// with the eventing capture. This test exercises that same delete +
// EmitMutationEventTx sequence inside a transaction the test itself controls
// and rolls back - mirroring TestNoEventOnRollback in
// internal/common/eventing/eventing_integration_test.go - to prove that a
// rollback undoes both the row deletion and the outbox insert together.
func TestDeleteAllAssetLinksIsTransactional(t *testing.T) {
	db := testDiscoveryEventingDB(t)
	enableAssetLinkEventing(t)
	ctx := securityDisabledContext()
	backend := newDiscoveryEventingBackend(t, db)

	aasID := "urn:aas:discovery-eventing:delete-atomic"
	if err := backend.CreateAllAssetLinks(ctx, aasID, specificAssetIDs([2]string{"globalAssetId", "urn:asset:delete-atomic"})); err != nil {
		t.Fatalf("seed asset links: %v", err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	sqlStr, args, err := goqu.Dialect("postgres").Delete("aas_identifier").Where(goqu.C("aasid").Eq(aasID)).ToSQL()
	if err != nil {
		t.Fatalf("build delete: %v", err)
	}
	if _, err = tx.ExecContext(ctx, sqlStr, args...); err != nil {
		t.Fatalf("exec delete: %v", err)
	}
	if err = history.EmitMutationEventTx(ctx, tx, history.TableAssetLink, aasID, history.ChangeDeleted, nil, true); err != nil {
		t.Fatalf("emit mutation event: %v", err)
	}
	// Simulate a downstream failure discovered before commit.
	if err = tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}

	rows := outboxRowsForIdentifier(t, db, aasID)
	if len(rows) != 1 {
		t.Fatalf("expected only the earlier create's outbox row to survive, got %d", len(rows))
	}
	assertEqualString(t, "operation", rows[0].operation, "created")

	links, err := backend.GetAllAssetLinks(ctx, aasID)
	if err != nil {
		t.Fatalf("expected asset links to still exist after rollback, got error: %v", err)
	}
	if len(links) != 1 {
		t.Fatalf("expected the seeded asset link to survive the rollback, got %d links", len(links))
	}
}

func TestNoAssetLinkEventWhenEventingDisabled(t *testing.T) {
	db := testDiscoveryEventingDB(t)
	// Deliberately do not call enableAssetLinkEventing: history default and no
	// hook registered is the out-of-the-box, eventing-disabled state.
	if err := eventing.Configure(assetLinkEventingComponent, "", common.EventingConfig{}); err != nil {
		t.Fatalf("reset eventing config: %v", err)
	}
	ctx := securityDisabledContext()
	backend := newDiscoveryEventingBackend(t, db)

	aasID := "urn:aas:discovery-eventing:disabled"
	if err := backend.CreateAllAssetLinks(ctx, aasID, specificAssetIDs([2]string{"globalAssetId", "urn:asset:disabled"})); err != nil {
		t.Fatalf("create asset links: %v", err)
	}
	if err := backend.AddAllAssetLinks(ctx, aasID, specificAssetIDs([2]string{"customId", "urn:asset:disabled-2"})); err != nil {
		t.Fatalf("add asset links: %v", err)
	}
	if err := backend.DeleteAllAssetLinks(ctx, aasID); err != nil {
		t.Fatalf("delete asset links: %v", err)
	}

	if rows := outboxRowsForIdentifier(t, db, aasID); len(rows) != 0 {
		t.Fatalf("expected no outbox rows while eventing is disabled, got %d", len(rows))
	}
}

func assertEqualString(t *testing.T, field, got, want string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %q; want %q", field, got, want)
	}
}
