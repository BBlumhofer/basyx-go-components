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

package eventing

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"sync"
	"testing"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

const testComponent = "submodel-repository"

// stubSink is a deterministic in-memory sink for relay tests.
type stubSink struct {
	name       string
	mu         sync.Mutex
	received   []Envelope
	calls      int
	failFirst  int
	alwaysFail bool
}

func (s *stubSink) Name() string { return s.name }

func (s *stubSink) Publish(_ context.Context, envelope Envelope) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.alwaysFail || s.calls <= s.failFirst {
		return errors.New("stub sink failure")
	}
	s.received = append(s.received, envelope)
	return nil
}

func (s *stubSink) Close(_ context.Context) error { return nil }

func (s *stubSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

func testDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("BASYX_EVENTING_TEST_DSN")
	if dsn == "" {
		t.Skip("BASYX_EVENTING_TEST_DSN not set; skipping eventing database test")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err = db.Ping(); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	truncateOutbox(t, db)
	return db
}

func truncateOutbox(t *testing.T, db *sql.DB) {
	t.Helper()
	if _, err := db.Exec("TRUNCATE event_outbox, event_outbox_sequence"); err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

func enableEventing(t *testing.T, sinks ...string) {
	t.Helper()
	history.Configure(history.Config{Mode: history.ModeOff})
	if err := Configure(testComponent, "urn:basyx:test", common.EventingConfig{
		Enabled:       true,
		OutboxEnabled: true,
		Format:        "cloudevents",
		TopicPrefix:   "basyx",
		Sinks:         sinks,
	}); err != nil {
		t.Fatalf("configure eventing: %v", err)
	}
	t.Cleanup(func() {
		_ = Configure(testComponent, "", common.EventingConfig{})
		history.Configure(history.Config{Mode: history.ModeOff})
	})
}

func appendSubmodel(t *testing.T, db *sql.DB, id string, changeType string) {
	t.Helper()
	err := common.ExecuteInTransaction(db, "", "", func(tx *sql.Tx) error {
		return history.AppendVersionTx(context.Background(), tx, history.TableSubmodel, id, changeType, nil, map[string]any{"id": id, "modelType": "Submodel"}, changeType == history.ChangeDeleted)
	})
	if err != nil {
		t.Fatalf("append %s: %v", changeType, err)
	}
}

func TestEnqueueViaHistorySeamWritesOutboxRow(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "stub")
	appendSubmodel(t, db, "sm-enqueue", history.ChangeCreated)

	var (
		operation, topic, pending, component string
		sequence                             int64
		payload                              []byte
	)
	row := db.QueryRow("SELECT operation, topic, pending_sinks, component, entity_sequence, payload FROM event_outbox WHERE identifier = $1", "sm-enqueue")
	if err := row.Scan(&operation, &topic, &pending, &component, &sequence, &payload); err != nil {
		t.Fatalf("scan outbox row: %v", err)
	}
	assertEqual(t, "operation", operation, "created")
	assertEqual(t, "topic", topic, "basyx/submodel-repository/submodel/created")
	assertEqual(t, "pending_sinks", pending, "stub")
	assertEqual(t, "component", component, testComponent)
	assertEqual(t, "sequence", sequence, int64(1))
	if len(payload) == 0 {
		t.Fatal("payload must not be empty")
	}
}

func TestNoEventOnRollback(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "stub")

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err = history.AppendVersionTx(context.Background(), tx, history.TableSubmodel, "sm-rollback", history.ChangeCreated, nil, map[string]any{"id": "sm-rollback"}, false); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	assertRowCount(t, db, "sm-rollback", 0)
}

func TestPerEntitySequenceIsMonotonic(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "stub")
	appendSubmodel(t, db, "sm-seq", history.ChangeCreated)
	appendSubmodel(t, db, "sm-seq", history.ChangeUpdated)
	appendSubmodel(t, db, "sm-seq", history.ChangeUpdated)

	rows, err := db.Query("SELECT entity_sequence FROM event_outbox WHERE identifier = $1 ORDER BY id", "sm-seq")
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	defer rows.Close()
	var sequences []int64
	for rows.Next() {
		var seq int64
		if err = rows.Scan(&seq); err != nil {
			t.Fatalf("scan: %v", err)
		}
		sequences = append(sequences, seq)
	}
	if len(sequences) != 3 || sequences[0] != 1 || sequences[1] != 2 || sequences[2] != 3 {
		t.Fatalf("unexpected sequences %v", sequences)
	}
}

func assertRowCount(t *testing.T, db *sql.DB, identifier string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM event_outbox WHERE identifier = $1", identifier).Scan(&count); err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != want {
		t.Fatalf("row count for %s = %d; want %d", identifier, count, want)
	}
}

func newTestRelay(db *sql.DB, sinks ...EventSink) *Relay {
	rc := ActiveConfig()
	rc.Relay.MaxAttempts = 3
	rc.Relay.BackoffBaseMs = 1000
	rc.Relay.BackoffMaxMs = 1000
	rc.Relay.PerEntityBatch = 64
	rc.Relay.EntityBatch = 128
	return &Relay{db: db, rc: rc, sinks: sinks, metrics: &Metrics{}}
}
