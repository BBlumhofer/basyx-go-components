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
	"log"
	"time"

	"github.com/doug-martin/goqu/v9"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

const cleanupBatchSize = 1000

func (r *Relay) runCleanup(ctx context.Context) {
	defer r.wg.Done()
	interval := time.Duration(cleanupIntervalMinutes(r.rc)) * time.Minute
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.cleanupCycle(ctx)
		}
	}
}

func (r *Relay) cleanupCycle(ctx context.Context) {
	published := r.deleteExpired(ctx, "published", publishedTTLHours(r.rc))
	dead := r.deleteExpired(ctx, "dead", deadLetterTTLHours(r.rc))
	if total := published + dead; total > 0 {
		r.metrics.addCleanupDeleted(total)
		log.Printf("EVENTING-CLEANUP removed=%d published=%d dead=%d", total, published, dead)
	}
}

// deleteExpired removes at most cleanupBatchSize rows in the given terminal state
// older than ttlHours, bounding lock duration on high-write deployments.
func (r *Relay) deleteExpired(ctx context.Context, status string, ttlHours int) int64 {
	if ttlHours <= 0 {
		return 0
	}
	expired := goqu.Dialect(common.Dialect).
		From(outboxTable).
		Select("id").
		Where(
			goqu.Ex{"status": status},
			goqu.L("db_created_at < NOW() - make_interval(hours => ?)", ttlHours),
		).
		Limit(cleanupBatchSize)
	query, args, err := goqu.Dialect(common.Dialect).
		Delete(outboxTable).
		Where(goqu.C("id").In(expired)).
		ToSQL()
	if err != nil {
		log.Printf("EVENTING-CLEANUP-BUILD %v", err)
		return 0
	}
	return r.execCleanup(ctx, query, args)
}

func (r *Relay) execCleanup(ctx context.Context, query string, args []any) int64 {
	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		log.Printf("EVENTING-CLEANUP-EXEC %v", err)
		return 0
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0
	}
	return affected
}

func cleanupIntervalMinutes(rc RuntimeConfig) int {
	if rc.Retention.CleanupIntervalMin <= 0 {
		return 30
	}
	return rc.Retention.CleanupIntervalMin
}

func publishedTTLHours(rc RuntimeConfig) int {
	if rc.Retention.PublishedTTLHours <= 0 {
		return 168
	}
	return rc.Retention.PublishedTTLHours
}

func deadLetterTTLHours(rc RuntimeConfig) int {
	return rc.Retention.DeadLetterTTLHours
}
