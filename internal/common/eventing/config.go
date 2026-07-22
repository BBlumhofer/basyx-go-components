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
	"strings"
	"sync"

	"github.com/eclipse-basyx/basyx-go-components/internal/common"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

// RuntimeConfig is the normalized, process-local eventing configuration.
type RuntimeConfig struct {
	Enabled       bool
	OutboxEnabled bool
	Component     string
	Source        string
	TopicPrefix   string
	Sinks         []string
	Relay         common.EventingRelayConfig
	Retention     common.EventingRetentionConfig
}

var (
	configMu     sync.RWMutex
	activeConfig RuntimeConfig
)

// Configure activates the process-local eventing configuration for a service.
//
// When eventing and its outbox are enabled it registers the in-transaction
// enqueue hook with the history append seam so every acknowledged CUD mutation
// writes an outbox row atomically with the model change. When disabled it clears
// the hook, restoring zero-overhead behavior.
//
// Parameters:
//   - component: Stable service/component name used on topics (e.g. "submodel-repository").
//   - source: CloudEvents source URI-reference (typically the service external URL).
//   - cfg: The eventing section of the loaded service configuration.
func Configure(component string, source string, cfg common.EventingConfig) error {
	runtime := RuntimeConfig{
		Enabled:       cfg.Enabled,
		OutboxEnabled: cfg.OutboxEnabled,
		Component:     normalizeComponent(component),
		Source:        normalizeSource(source, component),
		TopicPrefix:   normalizeTopicPrefix(cfg.TopicPrefix),
		Sinks:         normalizeSinks(cfg.Sinks),
		Relay:         cfg.Relay,
		Retention:     cfg.Retention,
	}
	configMu.Lock()
	activeConfig = runtime
	configMu.Unlock()

	if runtime.Enabled && runtime.OutboxEnabled {
		history.RegisterMutationEventHook(enqueueMutationEvent)
	} else {
		history.RegisterMutationEventHook(nil)
	}
	return nil
}

// ActiveConfig returns a copy of the current process-local eventing configuration.
func ActiveConfig() RuntimeConfig {
	configMu.RLock()
	defer configMu.RUnlock()
	return activeConfig
}

func normalizeComponent(component string) string {
	trimmed := strings.ToLower(strings.TrimSpace(component))
	if trimmed == "" {
		return "basyx"
	}
	return trimmed
}

func normalizeSource(source string, component string) string {
	trimmed := strings.TrimSpace(source)
	if trimmed != "" {
		return trimmed
	}
	return "urn:basyx:" + normalizeComponent(component)
}

func normalizeTopicPrefix(prefix string) string {
	trimmed := strings.Trim(strings.TrimSpace(prefix), "/")
	if trimmed == "" {
		return "basyx"
	}
	return trimmed
}

func normalizeSinks(sinks []string) []string {
	normalized := make([]string, 0, len(sinks))
	seen := make(map[string]bool, len(sinks))
	for _, sink := range sinks {
		name := strings.ToLower(strings.TrimSpace(sink))
		if name == "" || seen[name] {
			continue
		}
		seen[name] = true
		normalized = append(normalized, name)
	}
	return normalized
}
