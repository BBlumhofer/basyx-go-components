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
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
)

// buildTLSConfig builds a *tls.Config from the sink TLS settings, or returns nil
// when no TLS material and no insecure flag are configured.
func buildTLSConfig(cfg common.EventingTLSConfig) (*tls.Config, error) {
	if cfg.CAPath == "" && cfg.CertPath == "" && cfg.KeyPath == "" && !cfg.Insecure {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.Insecure} //nolint:gosec // insecure only when explicitly configured
	if cfg.CAPath != "" {
		pool, err := loadCAPool(cfg.CAPath)
		if err != nil {
			return nil, err
		}
		tlsCfg.RootCAs = pool
	}
	if cfg.CertPath != "" && cfg.KeyPath != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertPath, cfg.KeyPath)
		if err != nil {
			return nil, common.NewInternalServerError("EVENTING-TLS-KEYPAIR " + err.Error())
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	}
	return tlsCfg, nil
}

func loadCAPool(caPath string) (*x509.CertPool, error) {
	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		return nil, common.NewInternalServerError("EVENTING-TLS-CAREAD " + err.Error())
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		return nil, common.NewInternalServerError("EVENTING-TLS-CAPARSE could not parse CA bundle " + caPath)
	}
	return pool, nil
}
