using System.Text.Json;
using System.Text.RegularExpressions;
using Tech.Deployment.KafkaSetup.Models;

namespace Tech.Deployment.KafkaSetup.Services;

internal class KafkaJsonFileGenerator
{
    private readonly string _outputDir;
    public KafkaJsonFileGenerator(string outputDir = "/tmp/kafka-reassignments")
    {
        Directory.CreateDirectory(outputDir);
        _outputDir = outputDir;
    }

    public async Task<string> GenerateTopicsFile(HashSet<string> topics)
    {
        var topicsJson = new
        {
            version = 1,
            topics = topics.Select(t => new { topic = t }).ToList()
        };
        
        var topicsFile = Path.Combine(_outputDir, "topics.json");
        await File.WriteAllTextAsync(topicsFile, JsonSerializer.Serialize(topicsJson, new JsonSerializerOptions { WriteIndented = true }));

        return topicsFile;
    }

    public async Task<string> GenerateReassignmentsFile(List<TopicReassignmentJsonModel> partitions)
    {
        var reassignmentJson = new
        {
            version = 1,
            partitions
        };
        
        var reassignmentFile = Path.Combine(_outputDir, "reassignments.json");
        await File.WriteAllTextAsync(reassignmentFile, JsonSerializer.Serialize(reassignmentJson, new JsonSerializerOptions { WriteIndented = true }));

        return reassignmentFile;
    }
    
    /// <summary>
    /// Parses Kafka CLI output that includes "Current partition replica assignment"
    /// and generates a rollback JSON file that can be used with kafka-reassign-partitions.sh.
    /// </summary>
    public void CreateRollbackFile(string kafkaOutput)
    {
        // 1️⃣ Extract JSON block
        var jsonMatch = Regex.Match(
            kafkaOutput,
            @"\{""version""\s*:\s*\d+.*\]\s*\}",
            RegexOptions.Singleline
        );

        if (!jsonMatch.Success)
        {
            Console.WriteLine("❌ Could not extract JSON from Kafka output.");
            return;
        }

        var jsonString = jsonMatch.Value.Trim();

        try
        {
            using var doc = JsonDocument.Parse(jsonString);
            var partitionsElement = doc.RootElement.GetProperty("partitions");

            var partitions = partitionsElement
                             .EnumerateArray()
                             .Select(p =>
                                         new TopicReassignmentJsonModel(
                                             p.GetProperty("topic").GetString()!,
                                             p.GetProperty("partition").GetInt32(),
                                             p.GetProperty("replicas").EnumerateArray().Select(r => r.GetInt32()).ToList()
                                         )
                             )
                             .ToList();

            var rollbackObject = new
            {
                version = 1,
                partitions
            };

            var rollbackFile = Path.Combine(_outputDir, "reassignment_rollback.json");
            var formattedJson = JsonSerializer.Serialize(rollbackObject, new JsonSerializerOptions { WriteIndented = true });
            File.WriteAllText(rollbackFile, formattedJson);

            Console.WriteLine($"✅ Rollback file created successfully at: {rollbackFile}");
            Console.WriteLine("👉 To rollback, run:");
            Console.WriteLine($"""
                                  {CLIHelper.ChoseKafkaCommand("kafka-reassign-partitions")} \
                                      --bootstrap-server <broker> \
                                      --reassignment-json-file {rollbackFile} \
                                      --execute
                               """);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"❌ Failed to parse rollback JSON: {ex.Message}");
        }
    }
}