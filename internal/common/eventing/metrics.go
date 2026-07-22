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

import "sync/atomic"

// Metrics holds process-local eventing counters and gauges required for
// operational visibility (publish outcomes, retries, dead-letters, queue depth).
type Metrics struct {
	publishSuccess atomic.Int64
	publishFailure atomic.Int64
	retries        atomic.Int64
	deadLettered   atomic.Int64
	cleanupDeleted atomic.Int64
	pendingDepth   atomic.Int64
	oldestPending  atomic.Int64 // seconds
}

// MetricsSnapshot is an immutable view of the current metric values.
type MetricsSnapshot struct {
	PublishSuccess       int64 `json:"publishSuccess"`
	PublishFailure       int64 `json:"publishFailure"`
	Retries              int64 `json:"retries"`
	DeadLettered         int64 `json:"deadLettered"`
	CleanupDeleted       int64 `json:"cleanupDeleted"`
	PendingDepth         int64 `json:"pendingDepth"`
	OldestPendingSeconds int64 `json:"oldestPendingSeconds"`
}

func (m *Metrics) recordPublishSuccess()      { m.publishSuccess.Add(1) }
func (m *Metrics) recordPublishFailure()      { m.publishFailure.Add(1) }
func (m *Metrics) recordRetry()               { m.retries.Add(1) }
func (m *Metrics) recordDeadLetter()          { m.deadLettered.Add(1) }
func (m *Metrics) addCleanupDeleted(n int64)  { m.cleanupDeleted.Add(n) }
func (m *Metrics) setPendingDepth(n int64)    { m.pendingDepth.Store(n) }
func (m *Metrics) setOldestPending(sec int64) { m.oldestPending.Store(sec) }

// Snapshot returns the current metric values.
func (m *Metrics) Snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		PublishSuccess:       m.publishSuccess.Load(),
		PublishFailure:       m.publishFailure.Load(),
		Retries:              m.retries.Load(),
		DeadLettered:         m.deadLettered.Load(),
		CleanupDeleted:       m.cleanupDeleted.Load(),
		PendingDepth:         m.pendingDepth.Load(),
		OldestPendingSeconds: m.oldestPending.Load(),
	}
}
