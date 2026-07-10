using System.Diagnostics;
using System.Net;
using Confluent.Kafka;

namespace Kafka.Utilities;

public static class ConsumerHelpers
{
    internal static Activity EnrichWithMessagingTags<TKey, TValue>(
        this Activity activity,
        ConsumeResult<TKey, TValue> consumeResult,
        string groupId)
    {
        activity?.SetTag("messaging.system", "kafka");
        activity?.SetTag("messaging.destination_kind", "topic");
        activity?.SetTag("messaging.operation", "receive");
        activity?.SetTag("messaging.destination", consumeResult.Topic);
        activity?.SetTag("messaging.kafka.message_key", consumeResult.Message.Key.ToString());
        activity?.SetTag("messaging.kafka.partition", consumeResult.Partition.ToString());
        activity?.SetTag("messaging.kafka.offset", consumeResult.Offset.ToString());
        activity?.SetTag("messaging.kafka.consumer_group", groupId);
        return activity;
    }
}