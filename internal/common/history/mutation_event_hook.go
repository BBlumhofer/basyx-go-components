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

package history

import (
	"context"
	"database/sql"
	"sync"
)

// MutationEventFunc receives one committed logical mutation for downstream
// eventing. It runs inside the still-open model transaction and under the
// per-entity advisory lock already held by the append call, so an implementation
// can write a transactional outbox row atomically with the model change.
//
// The snapshot is the complete post-mutation resource for created/updated
// changes and the deletion representation for deleted changes.
type MutationEventFunc func(ctx context.Context, tx *sql.Tx, table string, identifier string, changeType string, snapshot map[string]any, deleted bool) error

var (
	mutationEventMu   sync.RWMutex
	mutationEventHook MutationEventFunc
)

// RegisterMutationEventHook installs (or clears with nil) the eventing hook that
// AppendVersionTx and AppendMutatedVersionTx invoke for each mutation. The
// eventing subsystem registers itself here at startup, keeping the history
// package free of any transport dependency.
func RegisterMutationEventHook(fn MutationEventFunc) {
	mutationEventMu.Lock()
	defer mutationEventMu.Unlock()
	mutationEventHook = fn
}

func activeMutationEventHook() MutationEventFunc {
	mutationEventMu.RLock()
	defer mutationEventMu.RUnlock()
	return mutationEventHook
}

// MutationEventHookActive reports whether a mutation event hook is registered.
func MutationEventHookActive() bool {
	return activeMutationEventHook() != nil
}

func emitMutationEventTx(ctx context.Context, tx *sql.Tx, table string, identifier string, changeType string, snapshot map[string]any, deleted bool) error {
	hook := activeMutationEventHook()
	if hook == nil {
		return nil
	}
	return hook(ctx, tx, table, identifier, changeType, snapshot, deleted)
}
