[![REUSE status](https://api.reuse.software/badge/github.com/SAP/kafka-operator)](https://api.reuse.software/info/github.com/SAP/kafka-operator)

# kafka-operator

## About this project

A Kubernetes operator to manage Kafka resources declaratively.

Today, the operator manages **Kafka topics**: for every `Topic` object in the cluster, the operator creates and continuously
reconciles the corresponding topic in the target Kafka cluster (partitions, replication, topic configuration), and deletes the
topic again when the object is removed. Further Kafka resource types (such as ACLs or users) may be added in the future.

The operator does not run or manage Kafka brokers. It talks to an existing Kafka cluster through the Kafka admin API; the
connection and authentication details are supplied per object through a referenced Kubernetes secret. That way, a single
operator instance can manage topics across many different Kafka clusters.

## Usage

### Topic custom resource

API group/version: `kafka.cs.sap.com/v1alpha1`, kind `Topic` (namespaced).

```yaml
apiVersion: kafka.cs.sap.com/v1alpha1
kind: Topic
metadata:
  name: orders
  namespace: my-namespace
spec:
  connectionSecretRef:
    name: my-kafka-connection
    key: connection.yaml
  name: orders.v1
  partitions: 6
  replicas: 3
  configs:
    retention.ms: "604800000"
    cleanup.policy: compact
```

Reconciliation state is reported in the object's status:

```yaml
status:
  observedGeneration: 1
  lastObservedAt: "2026-08-30T21:04:17Z"
  state: Ready
  conditions:
  - type: Ready
    status: "True"
    reason: Synced
    message: Topic synchronized successfully
    lastTransitionTime: "2026-08-30T21:04:17Z"
    observedGeneration: 1
  details:
    id: 9a1c2f...
    name: orders.v1
    partitions: 6
    replicas: 3
    configs:
      cleanup.policy: "*compact"
      retention.ms: "*604800000"
      segment.bytes: "1073741824"
```

The status fields are described in detail in [Status fields](#status-fields) below. Deletion of a `Topic` object deletes the
topic (and therefore all its data) in Kafka; the object is protected by a finalizer until the deletion has been confirmed by
the cluster.

#### Spec fields and defaulting

##### `spec.name`

The name of the topic in Kafka. If omitted, it defaults to `metadata.name` of the `Topic` object. The value must match
`^[a-zA-Z0-9._-]{1,249}$`.

The effective topic name is immutable. Once the object exists, the name can neither be changed nor be switched between the
explicit and the defaulted form in an incompatible way; the API server rejects such updates with `spec.name is immutable`.
Setting `spec.name` afterwards is only accepted if it equals `metadata.name`, and unsetting it is only accepted if the previous
value equaled `metadata.name`. To rename a topic, delete the object and create a new one (which destroys the topic's data).

##### `spec.partitions`

The number of partitions; defaults to `1` if omitted.

Kafka only supports increasing the partition count, therefore the value may only be increased. Updates that lower it are
rejected with `spec.partitions can only be increased`; this also covers removing the field once it was set to a value greater
than 1 (because removing it would fall back to the default of 1).

##### `spec.replicas`

The replication factor of the topic.

If omitted, the topic is created with the replication factor configured as default in the target Kafka cluster
(`default.replication.factor`), and the operator does not enforce any replication factor afterwards. If the field is set, the
operator actively aligns the partition assignments with the requested value, adding or removing replicas as needed.

Because the operator cannot reconstruct the cluster default retroactively, the field cannot be unset again once it was set;
such updates are rejected with `spec.replicas cannot be unset`. It can, however, be increased or decreased.

##### `spec.configs`

An optional map of Kafka topic configuration properties (such as `retention.ms`, `cleanup.policy`, `min.insync.replicas`, ...)
which override the corresponding broker defaults for this topic. Keys and values are passed to Kafka as-is, so all values have
to be given as strings.

The operator owns the set of overrides: entries added to `spec.configs` are set on the topic, changed entries are updated, and
entries removed from `spec.configs` are deleted again, so the topic falls back to the cluster default for that property.
Configuration properties which Kafka reports as sensitive are not supported.

The effective configuration of the topic is reported in `status.details.configs`; see [Status fields](#status-fields) below.

##### `spec.connectionSecretRef`

Required reference to the secret holding the connection details of the target Kafka cluster; see the next section.

#### Status fields

The status is maintained exclusively by the operator; it is never to be edited by users.

##### `status.state`

An aggregated, human readable summary of the reconciliation state of the topic:

| State | Meaning |
| --- | --- |
| `Pending` | The topic could not be processed yet (e.g. the connection secret does not exist yet); the operator retries. |
| `InProgress` | The topic was just seen for the first time, or was created/updated and is being re-checked. |
| `Ready` | The topic exists in the Kafka cluster and matches the spec. |
| `Failed` | Reconciliation failed with a non-retriable error; see the `Ready` condition's message. |
| `Deleting` | The object is being deleted, and the topic deletion has been triggered in the Kafka cluster. |

##### `status.conditions`

Conditions in the standard Kubernetes format (`type`, `status`, `reason`, `message`, `lastTransitionTime`,
`observedGeneration`). Currently there is exactly one condition of type `Ready`, which is `True` only if the topic is fully
synchronized. Its `reason` is one of `Unknown`, `FirstSeen`, `Processing`, `Synced`, `Deleting`, `Retrying`, `Error`; in error
and retry cases, `message` carries the underlying error text. State and condition changes are additionally emitted as
Kubernetes events on the object (of type `Warning` in state `Failed`, `Normal` otherwise).

##### `status.observedGeneration`

The `metadata.generation` of the object which was processed in the last reconciliation. As long as this value differs from
`metadata.generation`, the reported state refers to an outdated version of the spec. It is initialized with `-1`, meaning that
the object was not yet processed at all.

##### `status.lastObservedAt`

Timestamp of the last reconciliation of the object. Topics in state `Ready` are re-checked periodically (every 10 minutes), so
this timestamp also indicates how recent the reported details are.

##### `status.details`

The topic as it currently exists in the Kafka cluster (only populated in state `Ready`):

- `id`: the Kafka topic ID (hex-encoded UUID) as assigned by the cluster.
- `name`: the effective topic name (that is, `spec.name`, or `metadata.name` if `spec.name` is not set).
- `partitions`: the actual number of partitions of the topic.
- `replicas`: the actual replication factor of the topic. Note that this value **can be zero**: the replication factor is
  determined across all partitions, and if the partitions of the topic do not all have the same number of replicas (which can
  happen after failed or partial reassignments, or if the topic was modified outside of the operator), then no single
  replication factor exists, and `0` is reported instead.
- `configs`: the topic configuration.

`status.details.configs` contains **all** configuration properties of the topic as reported by the cluster — not only the ones
listed in `spec.configs`, but also all properties inherited from the broker/cluster defaults. Properties which are overridden
on topic level (i.e. those set through `spec.configs`) are marked with a **leading asterisk** prepended to their value. In the
example above, `cleanup.policy` (`"*compact"`) and `retention.ms` (`"*604800000"`) are topic-level overrides with the effective
values `compact` and `604800000`, while `segment.bytes` (`"1073741824"`) is inherited from the cluster defaults. The asterisk
is a marker only; it is not part of the actual configuration value.

### Connection secret

`spec.connectionSecretRef` points to a secret in the **same namespace** as the `Topic` object:

```yaml
spec:
  connectionSecretRef:
    # name of the secret (required)
    name: my-kafka-connection
    # key inside the secret's data (optional, defaults to "value")
    key: connection.yaml
```

If `key` is omitted, the operator falls back to the key `value`. If neither the secret nor the key exists, reconciliation is
retried (state `Pending`) until the secret shows up.

#### Format

The referenced key must contain a YAML (or JSON, since JSON is valid YAML) document with the following structure:

```yaml
# list of seed brokers, at least one entry is required
brokers:
- kafka-0.example.com:9093
- kafka-1.example.com:9093
# PEM-encoded CA bundle used to verify the broker certificates (optional)
caBundle: |
  -----BEGIN CERTIFICATE-----
  ...
  -----END CERTIFICATE-----
# PEM-encoded client certificate for mTLS (optional)
clientCertificate: |
  -----BEGIN CERTIFICATE-----
  ...
  -----END CERTIFICATE-----
# PEM-encoded private key belonging to clientCertificate (optional)
clientKey: |
  -----BEGIN PRIVATE KEY-----
  ...
  -----END PRIVATE KEY-----
# SASL mechanisms (optional)
sasl:
- scramSha512:
    username: my-user
    password: my-password
```

Rules enforced when parsing the connection details:

- `brokers` must contain at least one entry; entries have the usual `host:port` syntax.
- TLS is enabled as soon as at least one of `caBundle`, `clientCertificate`, `clientKey` is provided; the minimum TLS version
  is 1.2. If none of them is set, the operator connects in plaintext. Without `caBundle`, the system trust store is used.
- `clientCertificate` and `clientKey` must be specified together (client/mutual TLS authentication).
- `sasl` is a list of mechanism entries; each entry must specify **exactly one** of `plain`, `scramSha256` or `scramSha512`,
  and the chosen mechanism must have both `username` and `password` set.

Example secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: my-kafka-connection
  namespace: my-namespace
stringData:
  connection.yaml: |
    brokers:
    - kafka-0.example.com:9093
    caBundle: |
      -----BEGIN CERTIFICATE-----
      ...
      -----END CERTIFICATE-----
    sasl:
    - scramSha512:
        username: my-user
        password: my-password
```

Since the connection secret is looked up in the namespace of the `Topic` object, the topics of a given Kafka cluster can be
managed from any namespace which holds a suitable secret.

## Requirements and Setup

The recommended deployment method is to use the [Helm chart](https://github.com/sap/kafka-operator-helm):

```bash
helm upgrade -i kafka-operator oci://ghcr.io/sap/kafka-operator-helm/kafka-operator
```

## Documentation

The API reference is here: [https://pkg.go.dev/github.com/sap/kafka-operator](https://pkg.go.dev/github.com/sap/kafka-operator).

## Support, Feedback, Contributing

This project is open to feature requests/suggestions, bug reports etc. via [GitHub issues](https://github.com/SAP/kafka-operator/issues). Contribution and feedback are encouraged and always welcome. For more information about how to contribute, the project structure, as well as additional contribution information, see our [Contribution Guidelines](CONTRIBUTING.md).

## Security / Disclosure
If you find any bug that may be a security problem, please follow our instructions at [in our security policy](https://github.com/SAP/kafka-operator/security/policy) on how to report it. Please do not create GitHub issues for security-related doubts or problems.

## Code of Conduct

We as members, contributors, and leaders pledge to make participation in our community a harassment-free experience for everyone. By participating in this project, you agree to abide by its [Code of Conduct](https://github.com/SAP/.github/blob/main/CODE_OF_CONDUCT.md) at all times.

## Licensing

Copyright 2026 SAP SE or an SAP affiliate company and kafka-operator contributors. Please see our [LICENSE](LICENSE) for copyright and license information. Detailed information including third-party components and their licensing/copyright information is available [via the REUSE tool](https://api.reuse.software/info/github.com/SAP/kafka-operator).
