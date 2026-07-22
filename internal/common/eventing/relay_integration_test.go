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
	"testing"

	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

func TestRelayPublishesEntityInOrder(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "stub")
	appendSubmodel(t, db, "sm-order", history.ChangeCreated)
	appendSubmodel(t, db, "sm-order", history.ChangeUpdated)
	appendSubmodel(t, db, "sm-order", history.ChangeDeleted)

	sink := &stubSink{name: "stub"}
	relay := newTestRelay(db, sink)
	relay.dispatchCycle(context.Background())

	if sink.count() != 3 {
		t.Fatalf("expected 3 published, got %d", sink.count())
	}
	wantOrder := []string{
		"basyx/submodel-repository/submodel/created",
		"basyx/submodel-repository/submodel/updated",
		"basyx/submodel-repository/submodel/deleted",
	}
	for i, envelope := range sink.received {
		if envelope.Topic != wantOrder[i] {
			t.Fatalf("event %d topic = %q; want %q", i, envelope.Topic, wantOrder[i])
		}
	}
	assertStatusCount(t, db, "published", 3)
}

func TestRelayMarksDeadLetterAfterMaxAttempts(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "stub")
	appendSubmodel(t, db, "sm-dead", history.ChangeCreated)

	sink := &stubSink{name: "stub", alwaysFail: true}
	relay := newTestRelay(db, sink)
	relay.rc.Relay.MaxAttempts = 1
	relay.dispatchCycle(context.Background())

	assertStatusCount(t, db, "dead", 1)
	if got := relay.metrics.Snapshot().DeadLettered; got != 1 {
		t.Fatalf("dead-letter metric = %d; want 1", got)
	}
}

func TestRelayRetriesOnTransientFailure(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "stub")
	appendSubmodel(t, db, "sm-retry", history.ChangeCreated)

	sink := &stubSink{name: "stub", alwaysFail: true}
	relay := newTestRelay(db, sink)
	relay.rc.Relay.MaxAttempts = 5
	relay.dispatchCycle(context.Background())

	var status string
	var attempts int
	if err := db.QueryRow("SELECT status, attempts FROM event_outbox WHERE identifier = $1", "sm-retry").Scan(&status, &attempts); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if status != "pending" || attempts != 1 {
		t.Fatalf("status=%q attempts=%d; want pending/1", status, attempts)
	}
	if got := relay.metrics.Snapshot().Retries; got != 1 {
		t.Fatalf("retry metric = %d; want 1", got)
	}
}

func TestRelayPartialSinkDeliveryKeepsFailedSinkPending(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "ok", "bad")
	appendSubmodel(t, db, "sm-partial", history.ChangeCreated)

	okSink := &stubSink{name: "ok"}
	badSink := &stubSink{name: "bad", alwaysFail: true}
	relay := newTestRelay(db, okSink, badSink)
	relay.dispatchCycle(context.Background())

	if okSink.count() != 1 {
		t.Fatalf("ok sink deliveries = %d; want 1", okSink.count())
	}
	var status, pending string
	if err := db.QueryRow("SELECT status, pending_sinks FROM event_outbox WHERE identifier = $1", "sm-partial").Scan(&status, &pending); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if status != "pending" || pending != "bad" {
		t.Fatalf("status=%q pending=%q; want pending/bad", status, pending)
	}
}

func TestCleanupRemovesExpiredPublishedRows(t *testing.T) {
	db := testDB(t)
	enableEventing(t, "stub")
	appendSubmodel(t, db, "sm-cleanup", history.ChangeCreated)
	if _, err := db.Exec("UPDATE event_outbox SET status='published', published_at=NOW(), pending_sinks='', db_created_at = NOW() - make_interval(hours => 400)"); err != nil {
		t.Fatalf("age row: %v", err)
	}

	relay := newTestRelay(db)
	deleted := relay.deleteExpired(context.Background(), "published", 168)
	if deleted != 1 {
		t.Fatalf("deleted = %d; want 1", deleted)
	}
	assertStatusCount(t, db, "published", 0)
}

func assertStatusCount(t *testing.T, db *sql.DB, status string, want int) {
	t.Helper()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM event_outbox WHERE status = $1", status).Scan(&count); err != nil {
		t.Fatalf("count status %s: %v", status, err)
	}
	if count != want {
		t.Fatalf("status %s count = %d; want %d", status, count, want)
	}
}
