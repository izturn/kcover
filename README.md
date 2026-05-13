# kcover - Kubernetes Coverage for Fault Awareness and Recovery

Welcome to `kcover`, a Kubernetes solution designed to enhance the reliability and resilience of large-scale AI workloads by providing fault awareness and robust instant recovery mechanisms.

## Features

- **Fault Awareness**: Detect and respond to hardware, network, and software failures dynamically.
- **Instant Recovery**: Quickly restore operations without manual intervention, minimizing downtime and ensuring continuous training and service availability.
- **Scalability**: Designed for large-scale environments, handling complexities of distributed AI workloads.

## Getting Started

### Prerequisites

Ensure you have Kubernetes and Helm installed on your cluster. `kcover` is compatible with Kubernetes versions 1.19 and above.

### Installation

Install `kcover` using Helm:

```shell
helm repo add baizeai https://baizeai.github.io/charts
helm install kcover baizeai/kcover --namespace kcover-system --create-namespace
```

### Configuration

Configure `kcover` to monitor specific Kubernetes resources by labeling them:

```shell
kubectl label pytorchjobs <job-name> kcover.io/cascading-recovery=true
kubectl label pytorchjobs <job-name> kcover.io/need-recovery=true
```

`kcover` and `agent` read the current node name from the `NODE_NAME`
environment variable. Helm templates inject this automatically from
`spec.nodeName`. The legacy `FAST_RECOVERY_NODE_NAME` variable is still read in
code for backward compatibility during migration, but new deployments should use
`NODE_NAME` only.

## Agent Config

The agent supports loading its runtime configuration from a YAML file mounted
from a ConfigMap. The Helm chart creates a default ConfigMap automatically, and
you can also point the agent to an existing user-managed ConfigMap.

The only runtime flag kept by the agent is `--config`, which points to the
mounted configuration file. Business settings such as `interval`, `vendor`, and
all `metaX` thresholds are now read from the config file only.

Default chart-managed config:

```yaml
agent:
  config:
    data:
      vendor: 1
      interval: 5
      metaX:
        hcaIDs:
          - mlx5_0
          - mlx5_1
        day2CheckHour: 10
        gpuNum: 8
        temperature: 85
        eccMaxCount: 64
        ntpMaxOffsetMillis: 10
```

If `metaX.hcaIDs` is set, the agent runs `ibv_devinfo` and requires every
listed `hca_id` to have `state: PORT_ACTIVE (...)`.

Use a user-defined ConfigMap:

```yaml
agent:
  config:
    existingConfigMap: my-agent-config
    path: /etc/kcover-agent/config.yaml
```

## Usage

Once installed, `kcover` will automatically monitor the labeled resources for any signs of failures and perform recovery actions as specified in the configuration.

## Preflight Slow Node Detection

- The collector expects one preflight report per node.
- When `world_size` is present in the report, each report must contain exactly
  `world_size - 1` batches.
- For the common 16-node topology, this usually means 16 reports and 15
  batches per report, but this is a common case rather than a hardcoded rule.
- If `world_size` is missing, the collector first tries configured expected
  layout and otherwise infers the layout from the batch count in the report.
- Slow-node scoring uses a single threshold setting. It accepts either an
  absolute failed-batch count such as `8`, a ratio such as `0.5`, or a
  percentage such as `50%`.
- Agent-side node events carry a compacted preflight payload rather than the
  raw host report. The compacted payload keeps only manager-required fields:
  report identity plus per-batch `batch_idx`, `pair`, `self_ip`, and
  `status`.
- Incomplete report collections no longer wait forever. The controller expires
  stale job aggregations after the controller flag
  `--preflight-report-collection-timeout` and emits a warning event describing
  how many reports were received.

Supported `preflight-config` keys:

```yaml
data:
  BUSBW_THRESHOLD_GBPS: "5"
  SLOW_NODE_THRESHOLD: "50%"
  EXPECTED_REPORTS: "16"
  EXPECTED_BATCHES_PER_REPORT: "15"
```

Controller timeout example:

```yaml
controller:
  args:
    - --preflight-report-collection-timeout=30m
```

## Image Build Notes

The MetaX utility `mx-smi` is extracted into a dedicated image so that the
agent image no longer needs to reference the full `maca-pytorch` runtime
directly.

- Extracted image: `release-ci.daocloud.io/baize/mx-smi:v0.1`
- Agent base runtime: `ubuntu:24.04`
- Agent build arg: `MX_SMI_IMAGE=release-ci.daocloud.io/baize/mx-smi:v0.1`

Build and push the extracted `mx-smi` image:

```shell
make image-mx-smi
```

Build and push the agent image with the extracted `mx-smi` image injected:

```shell
make image-agent
```

If you need to build manually, use:

```shell
docker build -f docker/mx-smi.Dockerfile -t release-ci.daocloud.io/baize/mx-smi:v0.1 .
docker build -f docker/agent.Dockerfile --build-arg MX_SMI_IMAGE=release-ci.daocloud.io/baize/mx-smi:v0.1 -t release-ci.daocloud.io/baize/kcover-agent:<tag> .
```
