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
	"log"
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

// mqttInitialConnectGrace bounds how long sink construction waits for the
// first MQTT connection before giving up and returning control to the caller.
// It is a startup nicety only: autopaho keeps retrying in the background
// afterward, so a slow or momentarily unavailable broker at boot never blocks
// or fails the owning service - consistent with the rest of the eventing
// design, where broker unavailability is absorbed by retry, never surfaced as
// a request/startup failure. A var (not const) so tests can shorten it.
var mqttInitialConnectGrace = 10 * time.Second

const mqttSinkName = "mqtt"

type mqttSink struct {
	manager  *autopaho.ConnectionManager
	qos      byte
	retained bool
}

func newMQTTSink(ctx context.Context, cfg common.EventingMQTTConfig) (*mqttSink, error) {
	broker, err := url.Parse(cfg.BrokerURL)
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-MQTT-URL " + err.Error())
	}
	tlsCfg, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		return nil, err
	}
	clientCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{broker},
		TlsCfg:                        tlsCfg,
		KeepAlive:                     20,
		CleanStartOnInitialConnection: false,
		SessionExpiryInterval:         60,
		ConnectUsername:               cfg.Username,
		ConnectPassword:               []byte(cfg.Password),
		ClientConfig:                  paho.ClientConfig{ClientID: cfg.ClientID},
	}
	manager, err := autopaho.NewConnection(ctx, clientCfg)
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-MQTT-CONNECT " + err.Error())
	}
	awaitCtx, cancel := context.WithTimeout(ctx, mqttInitialConnectGrace)
	defer cancel()
	if err = manager.AwaitConnection(awaitCtx); err != nil {
		log.Printf("EVENTING-MQTT-AWAIT broker not reachable within %s, continuing startup; autopaho keeps retrying in the background: %v", mqttInitialConnectGrace, err)
	}
	qos := byte(cfg.QoS) //nolint:gosec // QoS is validated to 0..2 in configuration
	return &mqttSink{manager: manager, qos: qos, retained: cfg.Retained}, nil
}

func (s *mqttSink) Name() string { return mqttSinkName }

func (s *mqttSink) Publish(ctx context.Context, envelope Envelope) error {
	contentType := envelope.ContentType
	publish := &paho.Publish{
		Topic:      envelope.Topic,
		QoS:        s.qos,
		Retain:     s.retained,
		Payload:    envelope.Body,
		Properties: &paho.PublishProperties{ContentType: contentType},
	}
	if _, err := s.manager.Publish(ctx, publish); err != nil {
		return common.NewErrServiceUnavailable("EVENTING-MQTT-PUBLISH " + err.Error())
	}
	return nil
}

func (s *mqttSink) Close(ctx context.Context) error {
	if s.manager == nil {
		return nil
	}
	return s.manager.Disconnect(ctx)
}
