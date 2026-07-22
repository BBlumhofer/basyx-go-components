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
	"encoding/json"
	"testing"
	"time"

	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

func TestResourceForTable(t *testing.T) {
	cases := map[string]string{
		history.TableAAS:                "aas",
		history.TableSubmodel:           "submodel",
		history.TableConcept:            "concept-description",
		history.TableDescriptor:         "aas-descriptor",
		history.TableSubmodelDescriptor: "submodel-descriptor",
	}
	for table, want := range cases {
		got, err := ResourceForTable(table)
		if err != nil || got != want {
			t.Fatalf("ResourceForTable(%q) = %q, %v; want %q", table, got, err, want)
		}
	}
	if _, err := ResourceForTable("unknown_history"); err == nil {
		t.Fatal("expected error for unmapped table")
	}
}

func TestOperationForChangeType(t *testing.T) {
	cases := map[string]Operation{
		history.ChangeCreated: OperationCreated,
		history.ChangeUpdated: OperationUpdated,
		history.ChangeDeleted: OperationDeleted,
	}
	for changeType, want := range cases {
		got, err := operationForChangeType(changeType)
		if err != nil || got != want {
			t.Fatalf("operationForChangeType(%q) = %q, %v; want %q", changeType, got, err, want)
		}
	}
	if _, err := operationForChangeType("Weird"); err == nil {
		t.Fatal("expected error for unknown change type")
	}
}

func TestResolveTopicAndType(t *testing.T) {
	rc := RuntimeConfig{TopicPrefix: "basyx", Component: "submodel-repository"}
	if topic := resolveTopic(rc, "submodel", OperationUpdated); topic != "basyx/submodel-repository/submodel/updated" {
		t.Fatalf("unexpected topic %q", topic)
	}
	if ceType := cloudEventType("submodel", OperationCreated); ceType != "org.eclipse.basyx.submodel.created" {
		t.Fatalf("unexpected type %q", ceType)
	}
}

func TestNormalizeSinksDeduplicatesAndLowercases(t *testing.T) {
	got := normalizeSinks([]string{"MQTT", " kafka ", "mqtt", ""})
	if len(got) != 2 || got[0] != "mqtt" || got[1] != "kafka" {
		t.Fatalf("unexpected normalized sinks %v", got)
	}
}

func TestBackoffSecondsIsBoundedAndMonotonic(t *testing.T) {
	rc := RuntimeConfig{}
	rc.Relay.BackoffBaseMs = 1000
	rc.Relay.BackoffMaxMs = 8000
	previous := 0
	for attempt := 1; attempt <= 10; attempt++ {
		got := backoffSeconds(rc, attempt)
		if got < 1 || got > 8 {
			t.Fatalf("backoff out of bounds at attempt %d: %d", attempt, got)
		}
		if got < previous {
			t.Fatalf("backoff decreased at attempt %d: %d < %d", attempt, got, previous)
		}
		previous = got
	}
}

func TestBuildCloudEventStructure(t *testing.T) {
	rc := RuntimeConfig{Component: "submodel-repository", Source: "urn:basyx:submodel-repository", TopicPrefix: "basyx"}
	record := MutationRecord{
		EventID:        "11111111-1111-1111-1111-111111111111",
		Component:      "submodel-repository",
		Resource:       "submodel",
		Identifier:     "sm-1",
		Operation:      OperationCreated,
		EntitySequence: 1,
		OccurredAt:     time.Unix(1700000000, 0).UTC(),
		Data:           map[string]any{"id": "sm-1", "modelType": "Submodel"},
		Correlation:    Correlation{CorrelationID: "corr-1"},
	}
	payload, err := buildCloudEvent(rc, record)
	if err != nil {
		t.Fatalf("buildCloudEvent: %v", err)
	}
	var envelope map[string]any
	if err = json.Unmarshal(payload, &envelope); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}
	assertEqual(t, "specversion", envelope["specversion"], "1.0")
	assertEqual(t, "id", envelope["id"], record.EventID)
	assertEqual(t, "type", envelope["type"], "org.eclipse.basyx.submodel.created")
	assertEqual(t, "source", envelope["source"], rc.Source)
	assertEqual(t, "subject", envelope["subject"], "sm-1")
	assertEqual(t, "basyxcomponent", envelope["basyxcomponent"], "submodel-repository")
	assertEqual(t, "correlationid", envelope["correlationid"], "corr-1")
	data, ok := envelope["data"].(map[string]any)
	if !ok || data["id"] != "sm-1" {
		t.Fatalf("unexpected data payload: %v", envelope["data"])
	}
}

func assertEqual(t *testing.T, field string, got any, want any) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v; want %v", field, got, want)
	}
}
