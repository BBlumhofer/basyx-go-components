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

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

// buildSinks constructs the configured transports. On any failure it closes the
// sinks already created so no connection leaks.
func buildSinks(ctx context.Context, rc RuntimeConfig, cfg common.EventingConfig) ([]EventSink, error) {
	sinks := make([]EventSink, 0, len(rc.Sinks))
	for _, name := range rc.Sinks {
		sink, err := buildSink(ctx, name, cfg)
		if err != nil {
			closeSinks(ctx, sinks)
			return nil, err
		}
		sinks = append(sinks, sink)
	}
	return sinks, nil
}

func buildSink(ctx context.Context, name string, cfg common.EventingConfig) (EventSink, error) {
	switch name {
	case mqttSinkName:
		return newMQTTSink(ctx, cfg.MQTT)
	case kafkaSinkName:
		return newKafkaSink(ctx, cfg.Kafka)
	default:
		return nil, common.NewInternalServerError("EVENTING-SINK-UNKNOWN unsupported sink " + name)
	}
}

func closeSinks(ctx context.Context, sinks []EventSink) {
	for _, sink := range sinks {
		_ = sink.Close(ctx)
	}
}
