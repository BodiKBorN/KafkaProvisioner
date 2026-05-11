# KafkaProvisioner
Scans all Kafka topics defined in CorePlatformTopics Creates non-existing topics Increases partition counts Reassigns partition replicas across brokers Updates topic configs (retention, min ISR, unclean leader election) Runs as a one-shot deployment tool (not a long-running service)
