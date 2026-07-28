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
	"net"
	"strings"
	"testing"
	"time"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

// These smoke tests exercise the real Paho and franz-go sink code paths against
// in-process brokers (a mochi-mqtt broker and a kfake Kafka cluster). They speak
// the real broker protocols but require no Docker, so they run in any CI.

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	address := listener.Addr().String()
	_ = listener.Close()
	return address
}

func startEmbeddedMQTT(t *testing.T) (string, chan []byte) {
	t.Helper()
	address := freeTCPAddress(t)
	broker := mqtt.New(&mqtt.Options{InlineClient: true})
	if err := broker.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatalf("add auth hook: %v", err)
	}
	if err := broker.AddListener(listeners.NewTCP(listeners.Config{ID: "test", Address: address})); err != nil {
		t.Fatalf("add listener: %v", err)
	}
	go func() { _ = broker.Serve() }()
	received := make(chan []byte, 4)
	err := broker.Subscribe("basyx/#", 1, func(_ *mqtt.Client, _ packets.Subscription, pk packets.Packet) {
		received <- pk.Payload
	})
	if err != nil {
		t.Fatalf("inline subscribe: %v", err)
	}
	t.Cleanup(func() { _ = broker.Close() })
	return address, received
}

// TestSinkConstructionSurvivesUnreachableBrokerAtStartup guards against a
// regression observed running the multi-service eventing example: several
// services connecting to a broker at the same time occasionally missed the
// original hard 10s AwaitConnection/Ping deadline and crashed the entire
// service (EVENTING-MQTT-AWAIT / EVENTING-KAFKA-PING). Sink construction must
// never fail just because the broker isn't reachable yet at startup - the
// underlying client keeps retrying in the background, exactly like an
// in-flight publish failure does after startup.
func TestSinkConstructionSurvivesUnreachableBrokerAtStartup(t *testing.T) {
	previousMQTTGrace := mqttInitialConnectGrace
	previousKafkaGrace := kafkaInitialPingGrace
	mqttInitialConnectGrace = 200 * time.Millisecond
	kafkaInitialPingGrace = 200 * time.Millisecond
	t.Cleanup(func() {
		mqttInitialConnectGrace = previousMQTTGrace
		kafkaInitialPingGrace = previousKafkaGrace
	})

	unreachable := freeTCPAddress(t) // reserved, then closed - nothing listens here
	ctx := context.Background()

	mqttSink, err := newMQTTSink(ctx, common.EventingMQTTConfig{
		BrokerURL: "mqtt://" + unreachable,
		ClientID:  "basyx-unreachable-test",
		QoS:       1,
	})
	if err != nil {
		t.Fatalf("newMQTTSink must not fail when the broker is unreachable at startup, got: %v", err)
	}
	t.Cleanup(func() { _ = mqttSink.Close(ctx) })

	kSink, err := newKafkaSink(ctx, common.EventingKafkaConfig{
		Brokers: []string{unreachable},
		Topic:   "basyx.events",
	})
	if err != nil {
		t.Fatalf("newKafkaSink must not fail when the broker is unreachable at startup, got: %v", err)
	}
	t.Cleanup(func() { _ = kSink.Close(ctx) })
}

func TestMQTTSinkPublishesToEmbeddedBroker(t *testing.T) {
	address, received := startEmbeddedMQTT(t)
	ctx := context.Background()

	sink, err := newMQTTSink(ctx, common.EventingMQTTConfig{
		BrokerURL: "mqtt://" + address,
		ClientID:  "basyx-eventing-smoke",
		QoS:       1,
	})
	if err != nil {
		t.Fatalf("new mqtt sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close(ctx) })

	body := []byte(`{"specversion":"1.0","id":"evt-1"}`)
	err = sink.Publish(ctx, Envelope{
		Topic:       "basyx/submodel-repository/submodel/created",
		ContentType: CloudEventContentType,
		Body:        body,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case got := <-received:
		if string(got) != string(body) {
			t.Fatalf("received %q; want %q", got, body)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for MQTT delivery")
	}
}

func TestKafkaSinkPublishesToEmbeddedCluster(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.SeedTopics(1, "basyx.events"))
	if err != nil {
		t.Fatalf("new kfake cluster: %v", err)
	}
	t.Cleanup(cluster.Close)
	addrs := cluster.ListenAddrs()
	ctx := context.Background()

	sink, err := newKafkaSink(ctx, common.EventingKafkaConfig{
		Brokers:     addrs,
		Topic:       "basyx.events",
		Acks:        "all",
		Idempotent:  true,
		Compression: "lz4",
	})
	if err != nil {
		t.Fatalf("new kafka sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close(ctx) })

	body := []byte(`{"specversion":"1.0","id":"evt-2"}`)
	err = sink.Publish(ctx, Envelope{
		Component:    "submodel-repository",
		PartitionKey: "sm-kafka",
		ContentType:  CloudEventContentType,
		Body:         body,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	assertKafkaRecord(t, addrs, "basyx.events", "sm-kafka", body)
}

func TestEndToEndRelayDeliversToEmbeddedMQTT(t *testing.T) {
	db := testDB(t)
	address, received := startEmbeddedMQTT(t)
	enableEventing(t, "mqtt")
	appendSubmodel(t, db, "sm-e2e", history.ChangeCreated)

	ctx := context.Background()
	sink, err := newMQTTSink(ctx, common.EventingMQTTConfig{BrokerURL: "mqtt://" + address, ClientID: "basyx-e2e", QoS: 1})
	if err != nil {
		t.Fatalf("new mqtt sink: %v", err)
	}
	t.Cleanup(func() { _ = sink.Close(ctx) })

	relay := newTestRelay(db, sink)
	relay.dispatchCycle(ctx)

	select {
	case payload := <-received:
		if !strings.Contains(string(payload), "org.eclipse.basyx.submodel.created") {
			t.Fatalf("delivered payload missing event type: %s", payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for end-to-end MQTT delivery")
	}
	assertStatusCount(t, db, "published", 1)
}

func assertKafkaRecord(t *testing.T, brokers []string, topic string, wantKey string, wantValue []byte) {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		t.Fatalf("new consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fetches := consumer.PollFetches(ctx)
	if err = fetches.Err(); err != nil {
		t.Fatalf("poll fetches: %v", err)
	}
	records := fetches.Records()
	if len(records) == 0 {
		t.Fatal("no kafka records fetched")
	}
	record := records[0]
	if string(record.Key) != wantKey {
		t.Fatalf("record key = %q; want %q", record.Key, wantKey)
	}
	if string(record.Value) != string(wantValue) {
		t.Fatalf("record value = %q; want %q", record.Value, wantValue)
	}
}
