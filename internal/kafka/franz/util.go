/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package franz

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/sap/kafka-operator/pkg/api"
)

func newClient(id string, connectionDetails *api.ConnectionDetails) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(connectionDetails.Brokers...),
		kgo.ClientID(id),
	}

	if connectionDetails.CaBundle != "" || connectionDetails.ClientCertificate != "" || connectionDetails.ClientKey != "" {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if connectionDetails.CaBundle != "" {
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(connectionDetails.CaBundle)) {
				return nil, fmt.Errorf("no valid certificates found in CA bundle")
			}
			tlsCfg.RootCAs = pool
		}
		if connectionDetails.ClientCertificate != "" && connectionDetails.ClientKey != "" {
			cert, err := tls.X509KeyPair([]byte(connectionDetails.ClientCertificate), []byte(connectionDetails.ClientKey))
			if err != nil {
				return nil, fmt.Errorf("loading client certificate: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	for _, sasl := range connectionDetails.Sasl {
		if sasl.Plain != nil {
			opts = append(opts, kgo.SASL(plain.Auth{User: sasl.Plain.Username, Pass: sasl.Plain.Password}.AsMechanism()))
		}
		if sasl.ScramSha256 != nil {
			opts = append(opts, kgo.SASL(scram.Auth{User: sasl.ScramSha256.Username, Pass: sasl.ScramSha256.Password}.AsSha256Mechanism()))
		}
		if sasl.ScramSha512 != nil {
			opts = append(opts, kgo.SASL(scram.Auth{User: sasl.ScramSha512.Username, Pass: sasl.ScramSha512.Password}.AsSha512Mechanism()))
		}
	}

	return kgo.NewClient(opts...)
}

func newAdminClient(id string, connectionDetails *api.ConnectionDetails) (*kadm.Client, error) {
	cli, err := newClient(id, connectionDetails)
	if err != nil {
		return nil, err
	}
	return kadm.NewClient(cli), nil
}
