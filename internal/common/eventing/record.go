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
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// MutationRecord is the normalized, transport-neutral representation of one
// logical resource mutation, produced inside the model transaction.
type MutationRecord struct {
	EventID        string
	Component      string
	Resource       string
	EntityType     string
	Identifier     string
	Operation      Operation
	EntitySequence int64
	OccurredAt     time.Time
	Data           map[string]any // complete resource (created/updated) or deletion representation
	Correlation    Correlation
}

// Correlation carries request/actor context propagated from the audit context.
type Correlation struct {
	CorrelationID string
	RequestID     string
	ActorSubject  string
}

func identifierDigest(identifier string) string {
	sum := sha256.Sum256([]byte(identifier))
	return hex.EncodeToString(sum[:])
}

func resolveTopic(rc RuntimeConfig, resource string, operation Operation) string {
	return rc.TopicPrefix + "/" + rc.Component + "/" + resource + "/" + string(operation)
}

func cloudEventType(resource string, operation Operation) string {
	return "org.eclipse.basyx." + resource + "." + string(operation)
}
