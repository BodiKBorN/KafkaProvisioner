namespace Infrastructure.Options;

public class KafkaHostOptions
{
    public string Host { get; set; }
    public string EnvPrefix { get; set; }
    public int? MaxPollIntervalMs { get; set; } 
    public int? SessionTimeoutMs { get; set; } 
}