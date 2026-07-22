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

import "context"

// Envelope is one transport-neutral event handed to a sink for delivery.
type Envelope struct {
	Topic        string // hierarchical topic (used verbatim by MQTT)
	Component    string // producing component (used by Kafka per-component routing)
	PartitionKey string
	ContentType  string
	Body         []byte
	Headers      map[string]string
}

// EventSink delivers rendered events to a concrete transport. Publish must
// return nil only after the broker has acknowledged the message; a non-nil
// error causes the relay to retry the event with bounded backoff.
type EventSink interface {
	Name() string
	Publish(ctx context.Context, envelope Envelope) error
	Close(ctx context.Context) error
}
