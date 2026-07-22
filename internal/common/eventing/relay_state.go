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

func markRowPublished(ctx context.Context, tx *sql.Tx, id int64) error {
	query, args, err := goqu.Dialect(common.Dialect).
		Update(outboxTable).
		Set(goqu.Record{
			"status":        "published",
			"published_at":  goqu.L("NOW()"),
			"pending_sinks": "",
		}).
		Where(goqu.C("id").Eq(id)).
		ToSQL()
	if err != nil {
		return common.NewInternalServerError("EVENTING-MARKPUB-BUILD " + err.Error())
	}
	if _, err = tx.ExecContext(ctx, query, args...); err != nil {
		return common.NewInternalServerError("EVENTING-MARKPUB-EXEC " + err.Error())
	}
	return nil
}

func (r *Relay) markRowRetry(ctx context.Context, tx *sql.Tx, row outboxRow, remaining []string, pubErr error) error {
	attempts := row.attempts + 1
	update := goqu.Record{
		"attempts":      attempts,
		"pending_sinks": strings.Join(remaining, ","),
		"last_error":    truncateError(pubErr),
	}
	if attempts >= maxAttempts(r.rc) {
		update["status"] = "dead"
		r.metrics.recordDeadLetter()
	} else {
		update["next_attempt_at"] = goqu.L("NOW() + make_interval(secs => ?)", backoffSeconds(r.rc, attempts))
		r.metrics.recordRetry()
	}
	query, args, err := goqu.Dialect(common.Dialect).
		Update(outboxTable).
		Set(update).
		Where(goqu.C("id").Eq(row.id)).
		ToSQL()
	if err != nil {
		return common.NewInternalServerError("EVENTING-MARKRETRY-BUILD " + err.Error())
	}
	if _, err = tx.ExecContext(ctx, query, args...); err != nil {
		return common.NewInternalServerError("EVENTING-MARKRETRY-EXEC " + err.Error())
	}
	return nil
}

func (r *Relay) refreshDepthGauge(ctx context.Context) {
	query, args, err := goqu.Dialect(common.Dialect).
		From(outboxTable).
		Select(
			goqu.COUNT("*"),
			goqu.L("COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(db_created_at)))::bigint, 0)"),
		).
		Where(goqu.Ex{"status": "pending"}).
		ToSQL()
	if err != nil {
		return
	}
	var depth, oldest int64
	if err = r.db.QueryRowContext(ctx, query, args...).Scan(&depth, &oldest); err != nil {
		return
	}
	r.metrics.setPendingDepth(depth)
	r.metrics.setOldestPending(oldest)
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	const maxLen = 500
	if len(message) > maxLen {
		return message[:maxLen]
	}
	return message
}
