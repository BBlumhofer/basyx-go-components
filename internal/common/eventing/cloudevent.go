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
	cloudevents "github.com/cloudevents/sdk-go/v2"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

// CloudEventContentType is the media type of the structured CloudEvents envelope
// stored in the outbox and published by sinks.
const CloudEventContentType = "application/cloudevents+json"

// buildCloudEvent renders a MutationRecord as a structured CloudEvents 1.0 JSON
// envelope. Rendering happens once at enqueue time so publish retries are
// byte-identical and therefore idempotent for consumers keyed on the event id.
func buildCloudEvent(rc RuntimeConfig, rec MutationRecord) ([]byte, error) {
	event := cloudevents.NewEvent()
	event.SetID(rec.EventID)
	event.SetSource(rc.Source)
	event.SetType(cloudEventType(rec.Resource, rec.Operation))
	event.SetSubject(rec.Identifier)
	event.SetTime(rec.OccurredAt)
	event.SetExtension("basyxcomponent", rec.Component)
	event.SetExtension("basyxresource", rec.Resource)
	event.SetExtension("basyxsequence", rec.EntitySequence)
	if rec.Correlation.CorrelationID != "" {
		event.SetExtension("correlationid", rec.Correlation.CorrelationID)
	}
	if err := event.SetData(cloudevents.ApplicationJSON, rec.Data); err != nil {
		return nil, common.NewInternalServerError("EVENTING-CE-SETDATA " + err.Error())
	}
	payload, err := event.MarshalJSON()
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-CE-MARSHAL " + err.Error())
	}
	return payload, nil
}
