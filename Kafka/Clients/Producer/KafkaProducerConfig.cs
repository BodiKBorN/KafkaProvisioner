namespace Kafka.Clients.Producer;

public class KafkaProducerConfig 
{
    /// <summary>
    /// Pass the value to LingerMs (linger.ms) to producer configuration. If null - does not rewrite linger.ms default value.
    /// </summary>
    public double? LingerMs { get; set; }
}