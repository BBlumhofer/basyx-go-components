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
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

const kafkaSinkName = "kafka"

type kafkaSink struct {
	client            *kgo.Client
	topic             string
	topicPerComponent bool
}

func newKafkaSink(ctx context.Context, cfg common.EventingKafkaConfig) (*kafkaSink, error) {
	opts, err := kafkaOptions(cfg)
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-KAFKA-CLIENT " + err.Error())
	}
	if err = client.Ping(ctx); err != nil {
		client.Close()
		return nil, common.NewErrServiceUnavailable("EVENTING-KAFKA-PING " + err.Error())
	}
	return &kafkaSink{client: client, topic: cfg.Topic, topicPerComponent: cfg.TopicPerComponent}, nil
}

func kafkaOptions(cfg common.EventingKafkaConfig) ([]kgo.Opt, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(compressionCodec(cfg.Compression)),
	}
	if !cfg.Idempotent {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	tlsCfg, err := buildTLSConfig(cfg.TLS)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}
	mechanism, err := kafkaSASL(cfg.SASL)
	if err != nil {
		return nil, err
	}
	if mechanism != nil {
		opts = append(opts, kgo.SASL(mechanism))
	}
	return opts, nil
}

func compressionCodec(name string) kgo.CompressionCodec {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "zstd":
		return kgo.ZstdCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	default:
		return kgo.Lz4Compression()
	}
}

func (s *kafkaSink) Name() string { return kafkaSinkName }

func (s *kafkaSink) Publish(ctx context.Context, envelope Envelope) error {
	record := &kgo.Record{
		Topic:   s.resolveTopic(envelope.Component),
		Key:     []byte(envelope.PartitionKey),
		Value:   envelope.Body,
		Headers: kafkaHeaders(envelope),
	}
	if err := s.client.ProduceSync(ctx, record).FirstErr(); err != nil {
		return common.NewErrServiceUnavailable("EVENTING-KAFKA-PUBLISH " + err.Error())
	}
	return nil
}

func (s *kafkaSink) resolveTopic(component string) string {
	if s.topicPerComponent && component != "" {
		return s.topic + "." + component
	}
	return s.topic
}

func kafkaHeaders(envelope Envelope) []kgo.RecordHeader {
	headers := []kgo.RecordHeader{{Key: "content-type", Value: []byte(envelope.ContentType)}}
	for key, value := range envelope.Headers {
		headers = append(headers, kgo.RecordHeader{Key: key, Value: []byte(value)})
	}
	return headers
}

func (s *kafkaSink) Close(_ context.Context) error {
	if s.client != nil {
		s.client.Close()
	}
	return nil
}
