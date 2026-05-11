# KafkaProvisioner

A one-shot deployment tool that provisions and configures Kafka topics during the CI/CD pipeline. It runs as a Docker container at deploy time, ensures all required topics exist with the correct partition count, replication factor, and configuration — then exits.

## Overview

The tool performs the following steps on every deployment:

1. **Scans** all topics defined in `CorePlatformTopics` via reflection
2. **Creates** any topics that do not yet exist in Kafka
3. **Increases** partition count for topics that need more partitions
4. **Reassigns** partition replicas to match the target replication factor
5. **Updates** topic configurations (retention, min ISR, unclean leader election)

> ⚠️ This tool is **idempotent** — it is safe to run multiple times. Topics that are already correctly configured are skipped.

---

## Architecture

```
Program.cs
  ├── TopicsScanner            → Discovers all topics from CorePlatformTopics via reflection
  └── TopicConfigService       → Orchestrates topic creation, partition scaling, reassignment and config update
        ├── KafkaAdminClient         → Creates topics / increases partitions
        ├── KafkaJsonFileGenerator   → Generates reassignment JSON files for kafka-reassign-partitions CLI
        └── CLIHelper                → Executes shell commands (kafka-reassign-partitions)
```

---

## Configuration

### appsettings.json

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `KafkaHostOptions__Host` | `string` | `mock` | Comma-separated list of Kafka broker addresses |
| `KafkaHostOptions__EnvPrefix` | `string` | - | Environment prefix used for topic naming |
| `AutoExecuteEnabled` | `bool` | `true` | If `true`, partition reassignment is executed automatically. If `false`, only prints the commands to run manually |

### Environment Variables

```bash
KafkaHostOptions__Host=broker-1:9092,broker-2:9092,broker-3:9092
KafkaHostOptions__EnvPrefix=dev
ASPNETCORE_ENVIRONMENT=Development
AutoExecuteEnabled=true
```

### Per-Environment Settings

| Environment | `ASPNETCORE_ENVIRONMENT` |
|-------------|-------------------------|
| Development | `Development` |
| QA | `QA` |
| Production | `Production` |

---

## Topic Configuration Logic

### Replication Factor and Consistency Mode

The tool automatically determines the consistency mode based on the topic's replication factor:

| Replication Factor | Mode | `min.insync.replicas` | `unclean.leader.election.enable` |
|--------------------|------|-----------------------|----------------------------------|
| `<= 2` | Standard | `1` | `true` |
| `> 2` | Consistency | `2` | `false` |

### Retention

If a topic defines a `RetentionPeriod`, the `retention.ms` config is set accordingly. Existing custom configs on the topic are **preserved** unless explicitly overridden.

### Partition Reassignment

When the current replication factor of a topic does not match the target:

- **Increase:** Existing replicas are kept, new brokers are randomly selected from available ones
- **Decrease:** Replicas are trimmed to the target count (first N replicas kept)

Reassignment JSON files are written to `/tmp/kafka-reassignments/`:

| File | Description |
|------|-------------|
| `topics.json` | List of topics to reassign |
| `reassignments.json` | Target partition-to-broker mapping |
| `reassignment_rollback.json` | Auto-generated rollback plan from Kafka CLI output |

---

## Running Locally

### Prerequisites

- .NET 8 SDK
- Access to a Kafka cluster (or local Kafka via Docker)
- Java (required by Kafka CLI tools)

### Steps

```bash
# Set environment variables
export KafkaHostOptions__Host="localhost:9092"
export KafkaHostOptions__EnvPrefix="localtest"
export ASPNETCORE_ENVIRONMENT="Development"

# Run
cd KafkaProvisioner
dotnet run
```

### Using Launch Settings

The project includes a pre-configured launch profile pointing to the dev Kafka cluster:

```json
{
  "KafkaHostOptions__Host": "dev-platform-msk-main-0-broker-1.dev.ad.playdice.tech:9092,...",
  "KafkaHostOptions__EnvPrefix": "localtest"
}
```

---

## Docker

The Dockerfile uses a multi-stage build:

| Stage | Base Image | Purpose |
|-------|------------|---------|
| `base` | `mcr.microsoft.com/dotnet/sdk:8.0` | Runtime base |
| `kafka-cli` | `mcr.microsoft.com/dotnet/sdk:8.0` | Downloads Kafka 3.7.0 + Java |
| `build` | `base` | Restores and builds the .NET project |
| `publish` | `build` | Publishes the .NET output |
| `final` | `kafka-cli` | Combines .NET output + Kafka CLI binaries |

### Build and Run

```bash
docker build -f KafkaProvisioner/Dockerfile -t kafka-setup .

docker run --rm \
  -e KafkaHostOptions__Host="broker-1:9092,broker-2:9092" \
  -e KafkaHostOptions__EnvPrefix="dev" \
  -e ASPNETCORE_ENVIRONMENT="Development" \
  kafka-setup
```

---

## CI/CD Pipeline

The tool runs as a dedicated stage in the GitLab CI pipeline:

```
src-build → docker-build → kafka-setup → db-migration → deploy
```

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | All topics provisioned successfully |
| `1` | Setup failed — check logs for details |

---

## Adding a New Topic

Topics are defined in `CorePlatformTopics`. The tool discovers them automatically via reflection — **no changes to this project are needed** when a new topic is added.

Simply add the new `Topic` property to `CorePlatformTopics` and it will be picked up on the next deployment.

---

## Rollback

If a partition reassignment needs to be rolled back, use the auto-generated rollback file:

```bash
kafka-reassign-partitions.sh \
  --bootstrap-server <broker> \
  --reassignment-json-file /tmp/kafka-reassignments/reassignment_rollback.json \
  --execute
```

To verify reassignment progress:

```bash
kafka-reassign-partitions.sh \
  --bootstrap-server <broker> \
  --reassignment-json-file /tmp/kafka-reassignments/reassignments.json \
  --verify
```

---

## Dependencies

| Package | Version | Purpose |
|---------|---------|--------|
| `Confluent.Kafka` | `2.11.1` | Kafka Admin Client |

