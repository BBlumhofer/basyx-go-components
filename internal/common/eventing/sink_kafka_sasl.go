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

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

func kafkaSASL(cfg common.EventingSASLConfig) (sasl.Mechanism, error) {
	mechanism := strings.ToUpper(strings.TrimSpace(cfg.Mechanism))
	switch mechanism {
	case "", "NONE":
		return nil, nil
	case "PLAIN":
		return plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{User: cfg.Username, Pass: cfg.Password}, nil
		}), nil
	case "SCRAM-SHA-256":
		return scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.Username, Pass: cfg.Password}, nil
		}), nil
	case "SCRAM-SHA-512":
		return scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.Username, Pass: cfg.Password}, nil
		}), nil
	default:
		return nil, common.NewInternalServerError("EVENTING-KAFKA-SASL unsupported mechanism " + cfg.Mechanism)
	}
}
