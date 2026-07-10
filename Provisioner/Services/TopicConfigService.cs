using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Microsoft.Extensions.Options;
using System.Globalization;
using Tech.Deployment.KafkaSetup.Models;
using Infrastructure.Options;
using Kafka.Clients.Admin;

namespace Tech.Deployment.KafkaSetup.Services;

internal interface ITopicConfigService
{
    Task SetupTopicsAsync(List<Topic> topics);
}

internal class TopicConfigService(
    IOptions<KafkaHostOptions> kafkaHostOptions,
    IAdminClient adminClient,
    IKafkaAdminClient kafkaAdminClient,
    IConfiguration configuration) : ITopicConfigService
{
    private readonly bool _autoExecuteEnabled = configuration.GetValue<bool>("AutoExecuteEnabled");

    public async Task SetupTopicsAsync(List<Topic> topics)
    {
        var bootstrapServers = kafkaHostOptions.Value.Host;

        // Fetch all metadata
        Console.WriteLine($"📡 Fetching metadata from Kafka ({bootstrapServers})...");
        var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        var brokers = meta.Brokers.Select(b => b.BrokerId).ToList();
        Console.WriteLine($"Found brokers: {string.Join(", ", brokers)}");

        // Create topics and 
        await kafkaAdminClient.CreateNonExistingTopics(topics, meta);

        // Increase partitions in existed topics
        await kafkaAdminClient.IncreasePartitionsAmount(topics, meta);

        // Process each topic
        var allTopics = new HashSet<string>();
        var allPartitions = new List<TopicReassignmentJsonModel>();
        var allTopicsConfigs = new Dictionary<string, List<ConfigEntry>>();

        foreach (var topic in topics)
        {
            Console.WriteLine($"\nProcessing topic '{topic.Name}'...\n");

            var topicMeta = meta.Topics
                                .Where(t => !t.Error.IsError)
                                .FirstOrDefault(t => t.Topic == topic.Name);

            if (topicMeta is null)
            {
                Console.WriteLine($"No topic with name '{topic.Name}' found or it's newly created with correct configs. \n");
                continue;
            }

            var targetReplicationFactor = (int) topic.ReplicaFactor;

            if (brokers.Count < targetReplicationFactor)
                throw new Exception($"❌ Not enough brokers ({brokers.Count}) to reach replication factor {targetReplicationFactor} for '{topic.Name}'");

            // Configs
            allTopicsConfigs[topicMeta.Topic] = GenerateConfig(topic);
            
            Console.WriteLine($"Preparing topic '{topic.Name}' configs...");

            Console.WriteLine("📋 Input configs to update:");
            foreach (var cfg in allTopicsConfigs[topicMeta.Topic])
                Console.WriteLine($"   - {cfg.Name} = {cfg.Value}");

            // Generate Reassignment
            Console.WriteLine("📋 Start generation partitions reassignment...");
            var reassignments = GenerateReassignments(topicMeta, brokers, targetReplicationFactor);

            if (reassignments.Count == 0)
            {
                Console.WriteLine($"No partitions require reassignment for '{topicMeta.Topic}'.");
                continue;
            }

            allTopics.Add(topicMeta.Topic);
            allPartitions.AddRange(reassignments.Select(reassignment =>
                                                            new TopicReassignmentJsonModel(reassignment.Key.Topic, reassignment.Key.Partition.Value, reassignment.Value)));

        }

        await PerformReassignmentsAsync(bootstrapServers, brokers, allTopics, allPartitions);
        await UpdateTopicConfigAsync(allTopicsConfigs);
    }

    private static Dictionary<TopicPartition, List<int>> GenerateReassignments(
        TopicMetadata topicMeta,
        List<int> brokers,
        int targetReplicationFactor)
    {
        var reassignments = new Dictionary<TopicPartition, List<int>>();
        var random = new Random();

        foreach (var partition in topicMeta.Partitions)
        {
            var currentReplicas = partition.Replicas.ToList();
            var currentFactor = currentReplicas.Count;

            if (currentFactor == targetReplicationFactor)
            {
                Console.WriteLine($"  -Partition {partition.PartitionId}: already has {currentFactor} replicas, skipping.");
                continue;
            }

            var newReplicas = new List<int>();

            if (currentFactor < targetReplicationFactor)
            {
                // Increase Replication Factor
                newReplicas.AddRange(currentReplicas);
                var available = brokers.Except(currentReplicas).ToList();

                while (newReplicas.Count < targetReplicationFactor && available.Any())
                {
                    var nextBroker = available[random.Next(available.Count)];
                    newReplicas.Add(nextBroker);
                    available.Remove(nextBroker);
                }
            }
            else
            {
                // Decrease Replication Factor
                newReplicas = currentReplicas[..targetReplicationFactor];
            }

            if (newReplicas.Count == targetReplicationFactor)
            {
                reassignments[new TopicPartition(topicMeta.Topic, partition.PartitionId)] = newReplicas;
                Console.WriteLine($"  Partition {partition.PartitionId}: reassign to [{string.Join(",", newReplicas)}]");
            }
        }

        return reassignments;
    }

    private async Task PerformReassignmentsAsync(
        string bootstrapServers,
        List<int> brokers,
        HashSet<string> allTopics,
        List<TopicReassignmentJsonModel> allPartitions)
    {
        if (allPartitions.Count == 0)
        {
            Console.WriteLine("\n✅ No topics require reassignment — all replication factors are already sufficient.");
            return;
        }
        
        // --- Create JSON files ---
        Console.WriteLine("\nCreating JSON files...\n");

        var jsonFileGenerator = new KafkaJsonFileGenerator();
        var topicsFile = await jsonFileGenerator.GenerateTopicsFile(allTopics);
        var reassignmentFile = await jsonFileGenerator.GenerateReassignmentsFile(allPartitions);

        Console.WriteLine("✅ Files created:");
        Console.WriteLine($"   - {topicsFile}");
        Console.WriteLine($"   - {reassignmentFile}\n");

        Console.WriteLine("🎯 Done generating reassignment files.\n");

        var command = CLIHelper.ChoseKafkaCommand("kafka-reassign-partitions");

        var kafkaReassignPartitionGenerateCommand = $"""
                                                     {command} \
                                                         --bootstrap-server {bootstrapServers} \
                                                         --topics-to-move-json-file {topicsFile} \
                                                         --broker-list {string.Join(",", brokers)} \
                                                         --generate > generate-output.json
                                                     """;

        var kafkaReassignPartitionExecuteCommand = $"""
                                                    {command} \
                                                        --bootstrap-server {bootstrapServers} \
                                                        --reassignment-json-file {reassignmentFile} \
                                                        --execute
                                                    """;

        var kafkaReassignPartitionVerifyCommand = $"""
                                                   {command} \
                                                       --bootstrap-server {bootstrapServers} \
                                                       --reassignment-json-file {reassignmentFile} \
                                                       --verify
                                                   """;

        if (_autoExecuteEnabled)
        {
            Console.WriteLine($"❗ Executing kafka reassign partitions step:\n {kafkaReassignPartitionExecuteCommand}");
            var kafkaOutput = CLIHelper.RunShell(kafkaReassignPartitionExecuteCommand);
            Console.WriteLine($"✅ Result of executing:\n {kafkaOutput}");

            jsonFileGenerator.CreateRollbackFile(kafkaOutput);

            Console.WriteLine($"👉 To check the progress:\n {kafkaReassignPartitionVerifyCommand}");
        }
        else
        {
            Console.WriteLine("👉 Next step:");
            Console.WriteLine($"   {kafkaReassignPartitionGenerateCommand}");
            Console.WriteLine($"   {kafkaReassignPartitionExecuteCommand}\n");
        }
    }

    private static List<ConfigEntry> GenerateConfig(Topic topic)
    {
        var configEntries = new List<ConfigEntry>();

        var isConsistencyMode = (int) topic.ReplicaFactor > 2;

        configEntries.AddRange(
        [
            new ConfigEntry
            {
                IncrementalOperation = AlterConfigOpType.Set,
                Name = Constants.Constants.KafkaConfig.MinInSyncReplicas,
                Value = isConsistencyMode ? "2" : "1"
            },
            new ConfigEntry
            {
                IncrementalOperation = AlterConfigOpType.Set,
                Name = Constants.Constants.KafkaConfig.UncleanLeaderElectionEnable,
                Value = isConsistencyMode ? "false" : "true"
            }
        ]);

        if (topic.RetentionPeriod.HasValue)
            configEntries.Add(new ConfigEntry
            {
                IncrementalOperation = AlterConfigOpType.Set,
                Name = Constants.Constants.KafkaConfig.RetentionMs,
                Value = topic.RetentionPeriod.Value.TotalMilliseconds.ToString(CultureInfo.InvariantCulture)
            });
        
        return configEntries;
    }

    private async Task UpdateTopicConfigAsync(Dictionary<string, List<ConfigEntry>> allTopicsConfigs)
    {
        if (allTopicsConfigs.Count == 0)
            return;

        var finalConfiguration = new Dictionary<ConfigResource, List<ConfigEntry>>();

        Console.WriteLine("\nConfiguration update in progress...");
        
        try
        {
            foreach (var (topicName, configEntries) in allTopicsConfigs)
            {
                if (configEntries.Count == 0)
                    continue;

                await PopulateWithExistedCustomConfig(topicName, configEntries);

                finalConfiguration[new ConfigResource {Type = ResourceType.Topic, Name = topicName}] = configEntries;
            }

            await adminClient.AlterConfigsAsync(finalConfiguration);

            Console.WriteLine("✅ Configuration updated successfully.\n");
        }
        catch (KafkaException ex)
        {
            Console.WriteLine($"Kafka error: {ex.Error.Reason}\n");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}\n");
        }
    }

    private async Task PopulateWithExistedCustomConfig(string topicName, List<ConfigEntry> configs)
    {
        var existedConfigs = await DescribeTopicConfigs(topicName);
        var customConfigs = existedConfigs.Where(x => !x.Value.IsDefault && x.Key != Constants.Constants.KafkaConfig.MessageFormatVersion);

        configs.AddRange(customConfigs
                         .Where(x => !configs.Select(ce => ce.Name).Contains(x.Key))
                         .Select(customConfig => new ConfigEntry
                         {
                             Name = customConfig.Key,
                             Value = customConfig.Value.Value
                         }));
    }

    private async Task<Dictionary<string, ConfigEntryResult>> DescribeTopicConfigs(string topicName)
    {
        var result = await adminClient.DescribeConfigsAsync(
            new[]
            {
                new ConfigResource
                {
                    Type = ResourceType.Topic,
                    Name = topicName
                }
            });

        return result.First().Entries;
    }
}