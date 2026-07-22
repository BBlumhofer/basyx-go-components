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

// Package eventing implements transactional-outbox based publishing of Create,
// Update, and Delete events for BaSyx model resources. Mutations are captured
// inside the model transaction (via the history append seam), rendered as
// CloudEvents 1.0 envelopes, and delivered asynchronously to pluggable sinks
// (MQTT, Kafka) by a relay, decoupling broker availability from the write API.
package eventing

import (
	"github.com/eclipse-basyx/basyx-go-components/internal/common"
	"github.com/eclipse-basyx/basyx-go-components/internal/common/history"
)

// Operation names a CUD operation in the lower-case form used on topics and in
// CloudEvents types.
type Operation string

const (
	// OperationCreated marks a resource creation.
	OperationCreated Operation = "created"
	// OperationUpdated marks a resource update.
	OperationUpdated Operation = "updated"
	// OperationDeleted marks a resource deletion.
	OperationDeleted Operation = "deleted"
)

// resourceForTable maps a history table constant to the eventing resource name
// used on topics and in CloudEvents types.
var resourceForTable = map[string]string{
	history.TableAAS:                "aas",
	history.TableSubmodel:           "submodel",
	history.TableConcept:            "concept-description",
	history.TableDescriptor:         "aas-descriptor",
	history.TableSubmodelDescriptor: "submodel-descriptor",
}

// ResourceForTable returns the eventing resource name for a history table.
func ResourceForTable(table string) (string, error) {
	resource, ok := resourceForTable[table]
	if !ok {
		return "", common.NewInternalServerError("EVENTING-MAP-TABLE unmapped history table " + table)
	}
	return resource, nil
}

// operationForChangeType maps a history change type to an eventing operation.
func operationForChangeType(changeType string) (Operation, error) {
	switch changeType {
	case history.ChangeCreated:
		return OperationCreated, nil
	case history.ChangeUpdated:
		return OperationUpdated, nil
	case history.ChangeDeleted:
		return OperationDeleted, nil
	default:
		return "", common.NewInternalServerError("EVENTING-MAP-CHANGETYPE unknown change type " + changeType)
	}
}
