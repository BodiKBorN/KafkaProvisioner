using Microsoft.Extensions.Configuration;

namespace Tech.Kafka.IntegrationTests.Initialization.Configuration;

public class ConfigurationModel
{
    public string KafkaHost { get; init; }
}

public class ConfigurationHandler
{
    static ConfigurationHandler()
    {
        var config = new ConfigurationBuilder()
            .AddJsonFile("settings.json", false)
            .AddEnvironmentVariables()
            .Build();

        Instance = config.Get<ConfigurationModel>();
    }

    public static ConfigurationModel Instance { get; }
}