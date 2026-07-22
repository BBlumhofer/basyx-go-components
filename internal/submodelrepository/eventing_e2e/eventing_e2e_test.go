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

// Package eventing_e2e verifies the complete eventing runtime path: a real
// submodel repository persistence write is captured by the eventing hook, drained
// by the real relay started through eventing.Setup, and delivered to a live
// (embedded, in-process) MQTT broker. It needs a PostgreSQL database
// (BASYX_EVENTING_TEST_DSN) but no Docker.
package eventing_e2e

import (
	"context"
	"database/sql"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/FriedJannik/aas-go-sdk/jsonization"
	"github.com/FriedJannik/aas-go-sdk/types"
	_ "github.com/jackc/pgx/v5/stdlib"
	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/eventing"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
	persistence "github.com/eclipse-basyx/basyx-go-components/internal/submodelrepository/persistence"
)

const (
	testComponent = "submodel-repository"
	testSubmodel  = "https://example.com/ids/sm/eventing-e2e"
)

func TestSubmodelCreateAndDeleteDeliverEventsToBroker(t *testing.T) {
	dsn := os.Getenv("BASYX_EVENTING_TEST_DSN")
	if dsn == "" {
		t.Skip("BASYX_EVENTING_TEST_DSN not set; skipping eventing end-to-end test")
	}
	db := openDB(t, dsn)
	address, received := startEmbeddedMQTT(t)
	ctx := securityDisabledContext()

	startEventingRuntime(t, db, address)
	backend := newBackend(t, db)
	cleanupSubmodel(ctx, backend, db)

	if err := backend.CreateSubmodel(ctx, buildSubmodel(t)); err != nil {
		t.Fatalf("create submodel: %v", err)
	}
	assertDelivered(t, received, "org.eclipse.basyx.submodel.created")

	if err := backend.DeleteSubmodel(ctx, testSubmodel); err != nil {
		t.Fatalf("delete submodel: %v", err)
	}
	assertDelivered(t, received, "org.eclipse.basyx.submodel.deleted")
}

func startEventingRuntime(t *testing.T, db *sql.DB, brokerAddress string) {
	t.Helper()
	history.Configure(history.Config{Mode: history.ModeOff})
	cfg := common.EventingConfig{
		Enabled:       true,
		OutboxEnabled: true,
		Format:        "cloudevents",
		TopicPrefix:   "basyx",
		Sinks:         []string{"mqtt"},
		MQTT:          common.EventingMQTTConfig{BrokerURL: "mqtt://" + brokerAddress, ClientID: "basyx-e2e", QoS: 1},
	}
	cfg.Relay.PollIntervalMs = 100
	relay, err := eventing.Setup(context.Background(), db, testComponent, "urn:basyx:e2e", cfg)
	if err != nil {
		t.Fatalf("eventing setup: %v", err)
	}
	t.Cleanup(func() {
		eventing.ShutdownRelay(context.Background(), relay)
		_ = eventing.Configure(testComponent, "", common.EventingConfig{})
	})
}

func newBackend(t *testing.T, db *sql.DB) *persistence.SubmodelDatabase {
	t.Helper()
	backend, err := persistence.NewSubmodelDatabaseFromDB(db, nil, "permissive")
	if err != nil {
		t.Fatalf("new submodel backend: %v", err)
	}
	return backend
}

func buildSubmodel(t *testing.T) types.ISubmodel {
	t.Helper()
	submodel, err := jsonization.SubmodelFromJsonable(map[string]any{
		"id":        testSubmodel,
		"modelType": "Submodel",
		"idShort":   "EventingE2E",
	})
	if err != nil {
		t.Fatalf("build submodel: %v", err)
	}
	return submodel
}

func cleanupSubmodel(ctx context.Context, backend *persistence.SubmodelDatabase, db *sql.DB) {
	_ = backend.DeleteSubmodel(ctx, testSubmodel)
	_, _ = db.Exec("TRUNCATE event_outbox, event_outbox_sequence")
}

func assertDelivered(t *testing.T, received <-chan []byte, wantType string) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case payload := <-received:
			if strings.Contains(string(payload), wantType) && strings.Contains(string(payload), testSubmodel) {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %q event", wantType)
		}
	}
}

func securityDisabledContext() context.Context {
	return common.ContextWithConfig(context.Background(), &common.Config{})
}

func openDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	if err = db.Ping(); err != nil {
		t.Fatalf("ping db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func startEmbeddedMQTT(t *testing.T) (string, <-chan []byte) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()

	broker := mqtt.New(&mqtt.Options{InlineClient: true})
	if err = broker.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("add auth hook: %v", err)
	}
	if err = broker.AddListener(listeners.NewTCP(listeners.Config{ID: "e2e", Address: address})); err != nil {
		t.Fatalf("add listener: %v", err)
	}
	go func() { _ = broker.Serve() }()
	received := make(chan []byte, 16)
	if err = broker.Subscribe("basyx/#", 1, func(_ *mqtt.Client, _ packets.Subscription, pk packets.Packet) {
		received <- pk.Payload
	}); err != nil {
		t.Fatalf("inline subscribe: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	return address, received
}
