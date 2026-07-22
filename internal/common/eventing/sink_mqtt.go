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
	"net/url"
	"time"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

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
	awaitCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err = manager.AwaitConnection(awaitCtx); err != nil {
		return nil, common.NewInternalServerError("EVENTING-MQTT-AWAIT " + err.Error())
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
