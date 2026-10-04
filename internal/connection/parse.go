/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package connection

import (
	"fmt"

	"sigs.k8s.io/yaml"

	"github.com/sap/kafka-operator/pkg/api"
)

func ParseConnectionDetails(value []byte) (*api.ConnectionDetails, error) {
	connectionDetails := &api.ConnectionDetails{}
	if err := yaml.Unmarshal(value, connectionDetails); err != nil {
		return nil, err
	}
	if len(connectionDetails.Brokers) == 0 {
		return nil, fmt.Errorf("no brokers specified")
	}
	if connectionDetails.ClientCertificate == "" && connectionDetails.ClientKey != "" || connectionDetails.ClientCertificate != "" && connectionDetails.ClientKey == "" {
		return nil, fmt.Errorf("both client certificate and client key must be specified together")
	}
	for _, sasl := range connectionDetails.Sasl {
		switch {
		case sasl.Plain != nil:
			if sasl.Plain.Username == "" || sasl.Plain.Password == "" {
				return nil, fmt.Errorf("both SASL plain username and password must be specified together")
			}
			if sasl.ScramSha256 != nil || sasl.ScramSha512 != nil {
				return nil, fmt.Errorf("exactly one SASL method must be specified per entry")
			}
		case sasl.ScramSha256 != nil:
			if sasl.ScramSha256.Username == "" || sasl.ScramSha256.Password == "" {
				return nil, fmt.Errorf("both SASL SCRAM-SHA-256 username and password must be specified together")
			}
			if sasl.Plain != nil || sasl.ScramSha512 != nil {
				return nil, fmt.Errorf("exactly one SASL method must be specified per entry")
			}
		case sasl.ScramSha512 != nil:
			if sasl.ScramSha512.Username == "" || sasl.ScramSha512.Password == "" {
				return nil, fmt.Errorf("both SASL SCRAM-SHA-512 username and password must be specified together")
			}
			if sasl.Plain != nil || sasl.ScramSha256 != nil {
				return nil, fmt.Errorf("exactly one SASL method must be specified per entry")
			}
		default:
			return nil, fmt.Errorf("exactly one SASL method must be specified per entry")
		}
	}
	return connectionDetails, nil
}
