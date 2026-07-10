namespace Kafka.Clients.Admin;

public enum PartitionAmount
{
    Minimal = 1,
    Basic = 6,
    Low = 12,
    Medium = 24,
    High = 36
}

public enum ReplicaFactor
{
    Minimal = 1,
    Default = 3
}

public static class TopicRetentionPeriod
{
    public static readonly TimeSpan FewMinutes = TimeSpan.FromMinutes(5);
    public static readonly TimeSpan FewDays = TimeSpan.FromDays(2);
    public static readonly TimeSpan OneWeek = TimeSpan.FromDays(7);
    public static readonly TimeSpan TwoWeeks = TimeSpan.FromDays(14);
}

public record Topic(
    string Name, 
    Topic DeadLetterTopic,
    PartitionAmount PartitionsAmount,
    TimeSpan? RetentionPeriod,
    ReplicaFactor ReplicaFactor)
{
    public override string ToString()
    {
        return Name;
    }
}