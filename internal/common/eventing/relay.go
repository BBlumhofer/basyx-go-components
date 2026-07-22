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
	"log"
	"sync"
	"time"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

// Relay asynchronously drains the outbox and publishes events to sinks after the
// model transaction has committed, decoupling broker availability from the API.
type Relay struct {
	db      *sql.DB
	rc      RuntimeConfig
	sinks   []EventSink
	metrics *Metrics
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// StartRelay constructs the configured sinks and starts the dispatch and cleanup
// loops. It returns a nil relay (and nil error) when eventing or its outbox is
// disabled, so callers can defer Shutdown unconditionally.
func StartRelay(parent context.Context, db *sql.DB, cfg common.EventingConfig) (*Relay, error) {
	rc := ActiveConfig()
	if !rc.Enabled || !rc.OutboxEnabled {
		return nil, nil
	}
	if db == nil {
		return nil, common.NewInternalServerError("EVENTING-RELAY-NILDB database handle must not be nil")
	}
	sinks, err := buildSinks(parent, rc, cfg)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	relay := &Relay{db: db, rc: rc, sinks: sinks, metrics: &Metrics{}, cancel: cancel}
	relay.wg.Add(2)
	go relay.runDispatch(ctx)
	go relay.runCleanup(ctx)
	log.Printf("EVENTING-RELAY-START component=%s sinks=%v", rc.Component, rc.Sinks)
	return relay, nil
}

// Metrics returns a snapshot of the relay's operational counters.
func (r *Relay) Metrics() MetricsSnapshot {
	if r == nil {
		return MetricsSnapshot{}
	}
	return r.metrics.Snapshot()
}

// Shutdown stops the loops, waits for them to finish (bounded by ctx), and closes
// all sinks.
func (r *Relay) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.cancel()
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
	}
	return r.closeSinks(ctx)
}

func (r *Relay) closeSinks(ctx context.Context) error {
	var firstErr error
	for _, sink := range r.sinks {
		if err := sink.Close(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (r *Relay) runDispatch(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(durationMs(r.rc.Relay.PollIntervalMs, 250))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.dispatchCycle(ctx)
		}
	}
}

func (r *Relay) dispatchCycle(ctx context.Context) {
	digests, err := r.claimEntities(ctx)
	if err != nil {
		log.Printf("EVENTING-RELAY-CLAIM %v", err)
		return
	}
	for _, digest := range digests {
		if ctx.Err() != nil {
			return
		}
		if err := r.processEntity(ctx, digest); err != nil {
			log.Printf("EVENTING-RELAY-PROCESS digest=%s %v", digest, err)
		}
	}
	r.refreshDepthGauge(ctx)
}

func durationMs(value int, fallback int) time.Duration {
	if value <= 0 {
		value = fallback
	}
	return time.Duration(value) * time.Millisecond
}

func backoffSeconds(rc RuntimeConfig, attempts int) int {
	base := rc.Relay.BackoffBaseMs
	if base <= 0 {
		base = 1000
	}
	maxMs := rc.Relay.BackoffMaxMs
	if maxMs <= 0 {
		maxMs = 60000
	}
	delayMs := base
	for i := 1; i < attempts && delayMs < maxMs; i++ {
		delayMs *= 2
	}
	if delayMs > maxMs {
		delayMs = maxMs
	}
	seconds := delayMs / 1000
	if seconds < 1 {
		seconds = 1
	}
	return seconds
}

func maxAttempts(rc RuntimeConfig) int {
	if rc.Relay.MaxAttempts <= 0 {
		return 12
	}
	return rc.Relay.MaxAttempts
}

func perEntityBatch(rc RuntimeConfig) int {
	if rc.Relay.PerEntityBatch <= 0 {
		return 64
	}
	return rc.Relay.PerEntityBatch
}

func entityBatch(rc RuntimeConfig) int {
	if rc.Relay.EntityBatch <= 0 {
		return 128
	}
	return rc.Relay.EntityBatch
}

// safeLimit converts a bounded, non-negative batch size to the uint expected by
// the query builder without risking a negative-to-uint overflow.
func safeLimit(n int) uint {
	if n < 0 {
		return 0
	}
	return uint(n) //nolint:gosec // n is guaranteed non-negative above
}
