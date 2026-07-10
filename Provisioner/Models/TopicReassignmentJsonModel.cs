namespace Tech.Deployment.KafkaSetup.Models;

public record TopicReassignmentJsonModel(string topic, int partition, List<int> replicas);