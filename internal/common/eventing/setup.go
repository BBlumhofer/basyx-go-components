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
	"time"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

const shutdownTimeout = 10 * time.Second

// Setup activates eventing for a service and starts its relay in one call. It is
// safe to call when eventing is disabled: it returns a nil relay and no error,
// and ShutdownRelay tolerates a nil relay.
//
// Parameters:
//   - ctx: Service root context (from common.SignalContext) driving relay shutdown.
//   - db: Shared database handle used by the relay to drain the outbox.
//   - component: Stable component name used on topics (e.g. "submodel-repository").
//   - source: CloudEvents source URI-reference (typically the external URL).
//   - cfg: The eventing section of the service configuration.
func Setup(ctx context.Context, db *sql.DB, component string, source string, cfg common.EventingConfig) (*Relay, error) {
	if err := Configure(component, source, cfg); err != nil {
		return nil, err
	}
	return StartRelay(ctx, db, cfg)
}

// ShutdownRelay stops the relay using a bounded context derived from ctx so any
// context values are preserved while the model context's cancellation does not
// abort sink drainage prematurely.
func ShutdownRelay(ctx context.Context, relay *Relay) {
	if relay == nil {
		return
	}
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownTimeout)
	defer cancel()
	_ = relay.Shutdown(shutdownCtx)
}
