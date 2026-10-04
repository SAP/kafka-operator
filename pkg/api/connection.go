/*
SPDX-FileCopyrightText: 2026 SAP SE or an SAP affiliate company and kafka-operator contributors
SPDX-License-Identifier: Apache-2.0
*/

package api

type ConnectionDetails struct {
	Brokers           []string        `json:"brokers"`
	CaBundle          string          `json:"caBundle,omitempty"`
	ClientCertificate string          `json:"clientCertificate,omitempty"`
	ClientKey         string          `json:"clientKey,omitempty"`
	Sasl              []SaslMechanism `json:"sasl,omitempty"`
}

type SaslMechanism struct {
	Plain       *SaslPlain       `json:"plain,omitempty"`
	ScramSha256 *SaslScramSha256 `json:"scramSha256,omitempty"`
	ScramSha512 *SaslScramSha512 `json:"scramSha512,omitempty"`
}

type SaslPlain struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SaslScramSha256 struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type SaslScramSha512 struct {
	Username string `json:"username"`
	Password string `json:"password"`
}
