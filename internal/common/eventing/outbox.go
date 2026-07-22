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
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/google/uuid"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

const outboxTable = "event_outbox"

// enqueueMutationEvent is the history mutation hook. It runs inside the model
// transaction, under the per-entity advisory lock, and writes exactly one
// outbox row so publication is atomic with the model change.
func enqueueMutationEvent(ctx context.Context, tx *sql.Tx, table string, identifier string, changeType string, snapshot map[string]any, deleted bool) error {
	rc := ActiveConfig()
	if !rc.Enabled || !rc.OutboxEnabled {
		return nil
	}
	record, err := buildMutationRecord(ctx, tx, rc, table, identifier, changeType, snapshot, deleted)
	if err != nil {
		return err
	}
	payload, err := buildCloudEvent(rc, record)
	if err != nil {
		return err
	}
	return insertOutboxRowTx(ctx, tx, rc, record, payload)
}

func buildMutationRecord(ctx context.Context, tx *sql.Tx, rc RuntimeConfig, table string, identifier string, changeType string, snapshot map[string]any, deleted bool) (MutationRecord, error) {
	resource, err := ResourceForTable(table)
	if err != nil {
		return MutationRecord{}, err
	}
	operation, err := operationForChangeType(changeType)
	if err != nil {
		return MutationRecord{}, err
	}
	sequence, err := nextEntitySequenceTx(ctx, tx, rc.Component, identifier)
	if err != nil {
		return MutationRecord{}, err
	}
	audit := history.FromContext(ctx)
	return MutationRecord{
		EventID:        uuid.NewString(),
		Component:      rc.Component,
		Resource:       resource,
		EntityType:     table,
		Identifier:     identifier,
		Operation:      operation,
		EntitySequence: sequence,
		OccurredAt:     time.Now().UTC(),
		Data:           eventData(identifier, resource, snapshot, deleted),
		Correlation: Correlation{
			CorrelationID: audit.CorrelationID,
			RequestID:     audit.RequestID,
			ActorSubject:  audit.ActorSubject,
		},
	}, nil
}

func eventData(identifier string, resource string, snapshot map[string]any, deleted bool) map[string]any {
	if !deleted && snapshot != nil {
		return snapshot
	}
	return map[string]any{"id": identifier, "entityType": resource, "deleted": true}
}

func nextEntitySequenceTx(ctx context.Context, tx *sql.Tx, component string, identifier string) (int64, error) {
	digest := identifierDigest(identifier)
	query, args, err := goqu.Dialect(common.Dialect).
		Insert("event_outbox_sequence").
		Rows(goqu.Record{
			"component":         component,
			"identifier_digest": digest,
			"identifier":        identifier,
			"last_sequence":     1,
		}).
		OnConflict(goqu.DoUpdate("component,identifier_digest", goqu.Record{
			"last_sequence": goqu.L("event_outbox_sequence.last_sequence + 1"),
			"db_updated_at": goqu.L("NOW()"),
		})).
		Returning("last_sequence").
		ToSQL()
	if err != nil {
		return 0, common.NewInternalServerError("EVENTING-SEQ-BUILD " + err.Error())
	}
	var sequence int64
	if err = tx.QueryRowContext(ctx, query, args...).Scan(&sequence); err != nil {
		return 0, common.NewInternalServerError("EVENTING-SEQ-EXEC " + err.Error())
	}
	return sequence, nil
}

func insertOutboxRowTx(ctx context.Context, tx *sql.Tx, rc RuntimeConfig, record MutationRecord, payload []byte) error {
	query, args, err := goqu.Dialect(common.Dialect).
		Insert(outboxTable).
		Rows(goqu.Record{
			"event_id":          record.EventID,
			"component":         record.Component,
			"entity_type":       record.EntityType,
			"identifier":        record.Identifier,
			"identifier_digest": identifierDigest(record.Identifier),
			"operation":         string(record.Operation),
			"entity_sequence":   record.EntitySequence,
			"occurred_at":       record.OccurredAt,
			"correlation_id":    record.Correlation.CorrelationID,
			"request_id":        record.Correlation.RequestID,
			"actor_subject":     record.Correlation.ActorSubject,
			"topic":             resolveTopic(rc, record.Resource, record.Operation),
			"partition_key":     record.Identifier,
			"content_type":      CloudEventContentType,
			"payload":           goqu.L("?::jsonb", string(payload)),
			"pending_sinks":     strings.Join(rc.Sinks, ","),
		}).
		ToSQL()
	if err != nil {
		return common.NewInternalServerError("EVENTING-ENQUEUE-BUILD " + err.Error())
	}
	if _, err = tx.ExecContext(ctx, query, args...); err != nil {
		return common.NewInternalServerError("EVENTING-ENQUEUE-EXEC " + err.Error())
	}
	return nil
}
