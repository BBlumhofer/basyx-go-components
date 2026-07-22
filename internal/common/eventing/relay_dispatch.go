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
	"strings"

	"github.com/doug-martin/goqu/v9"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

type outboxRow struct {
	id           int64
	eventID      string
	component    string
	topic        string
	partitionKey string
	contentType  string
	payload      []byte
	pendingSinks []string
	attempts     int
}

// claimEntities returns due entity digests, ordered by their oldest pending row,
// so no single busy entity starves the others.
func (r *Relay) claimEntities(ctx context.Context) ([]string, error) {
	query, args, err := goqu.Dialect(common.Dialect).
		From(outboxTable).
		Select("identifier_digest").
		Where(goqu.Ex{"status": "pending"}, goqu.L("next_attempt_at <= NOW()")).
		GroupBy("identifier_digest").
		Order(goqu.L("MIN(id)").Asc()).
		Limit(safeLimit(entityBatch(r.rc))).
		ToSQL()
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-CLAIM-BUILD " + err.Error())
	}
	rows, err := r.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-CLAIM-EXEC " + err.Error())
	}
	defer func() { _ = rows.Close() }()
	var digests []string
	for rows.Next() {
		var digest string
		if err = rows.Scan(&digest); err != nil {
			return nil, common.NewInternalServerError("EVENTING-CLAIM-SCAN " + err.Error())
		}
		digests = append(digests, digest)
	}
	return digests, rows.Err()
}

// processEntity drains one entity under a per-entity advisory lock so ordering is
// preserved across workers and service replicas. Delivery happens in insertion
// order; a failing event halts that entity until the next cycle.
func (r *Relay) processEntity(ctx context.Context, digest string) (err error) {
	tx, txErr := r.db.BeginTx(ctx, nil)
	if txErr != nil {
		return common.NewInternalServerError("EVENTING-PROCESS-BEGIN " + txErr.Error())
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if commitErr := tx.Commit(); commitErr != nil {
			err = common.NewInternalServerError("EVENTING-PROCESS-COMMIT " + commitErr.Error())
		}
	}()

	locked, err := tryAdvisoryLock(ctx, tx, digest)
	if err != nil || !locked {
		return err
	}
	rows, err := loadPendingRows(ctx, tx, digest, perEntityBatch(r.rc))
	if err != nil {
		return err
	}
	for _, row := range rows {
		advance, rowErr := r.deliverRow(ctx, tx, row)
		if rowErr != nil {
			return rowErr
		}
		if !advance {
			break
		}
	}
	return nil
}

func tryAdvisoryLock(ctx context.Context, tx *sql.Tx, digest string) (bool, error) {
	query, args, err := goqu.Dialect(common.Dialect).
		Select(goqu.L("pg_try_advisory_xact_lock(hashtext(?)::bigint)", digest)).
		ToSQL()
	if err != nil {
		return false, common.NewInternalServerError("EVENTING-LOCK-BUILD " + err.Error())
	}
	var locked bool
	if err = tx.QueryRowContext(ctx, query, args...).Scan(&locked); err != nil {
		return false, common.NewInternalServerError("EVENTING-LOCK-EXEC " + err.Error())
	}
	return locked, nil
}

func loadPendingRows(ctx context.Context, tx *sql.Tx, digest string, limit int) ([]outboxRow, error) {
	query, args, err := goqu.Dialect(common.Dialect).
		From(outboxTable).
		Select("id", "event_id", "component", "topic", "partition_key", "content_type", "payload", "pending_sinks", "attempts").
		Where(goqu.Ex{"identifier_digest": digest, "status": "pending"}).
		Order(goqu.C("id").Asc()).
		Limit(safeLimit(limit)).
		ToSQL()
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-LOAD-BUILD " + err.Error())
	}
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-LOAD-EXEC " + err.Error())
	}
	defer func() { _ = rows.Close() }()
	return scanOutboxRows(rows)
}

func scanOutboxRows(rows *sql.Rows) ([]outboxRow, error) {
	var result []outboxRow
	for rows.Next() {
		var row outboxRow
		var pending string
		if err := rows.Scan(&row.id, &row.eventID, &row.component, &row.topic, &row.partitionKey, &row.contentType, &row.payload, &pending, &row.attempts); err != nil {
			return nil, common.NewInternalServerError("EVENTING-LOAD-SCAN " + err.Error())
		}
		row.pendingSinks = splitSinks(pending)
		result = append(result, row)
	}
	return result, rows.Err()
}

func splitSinks(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// deliverRow publishes one row to its still-pending sinks and records the result.
// The boolean reports whether the relay may advance to the next event of the
// entity (true only when the row was fully delivered).
func (r *Relay) deliverRow(ctx context.Context, tx *sql.Tx, row outboxRow) (bool, error) {
	remaining, pubErr := r.publishToSinks(ctx, row)
	if len(remaining) == 0 {
		r.metrics.recordPublishSuccess()
		return true, markRowPublished(ctx, tx, row.id)
	}
	r.metrics.recordPublishFailure()
	return false, r.markRowRetry(ctx, tx, row, remaining, pubErr)
}

func (r *Relay) publishToSinks(ctx context.Context, row outboxRow) ([]string, error) {
	var remaining []string
	var firstErr error
	for _, name := range row.pendingSinks {
		sink := r.sinkByName(name)
		if sink == nil {
			continue // sink removed from config; treat as delivered to avoid blocking
		}
		envelope := Envelope{
			Topic:        row.topic,
			Component:    row.component,
			PartitionKey: row.partitionKey,
			ContentType:  row.contentType,
			Body:         row.payload,
			Headers:      map[string]string{"ce-id": row.eventID},
		}
		if err := sink.Publish(ctx, envelope); err != nil {
			remaining = append(remaining, name)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return remaining, firstErr
}

func (r *Relay) sinkByName(name string) EventSink {
	for _, sink := range r.sinks {
		if sink.Name() == name {
			return sink
		}
	}
	return nil
}
