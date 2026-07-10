using Infrastructure.Options;
using Tech.Kafka.Clients.Admin;
using Tech.Social.Infrastructure.Bus.Abstractions;

namespace Tech.Social.Infrastructure.Bus;

public class CorePlatformTopics : ICorePlatformTopics
{
    private const string CorePlatformNamespace = "CorePlatform";
    private readonly string _domainNamespace;

    public CorePlatformTopics(KafkaHostOptions kafkaOptions)
    {
        if (string.IsNullOrEmpty(kafkaOptions.EnvPrefix))
        {
            _domainNamespace = CorePlatformNamespace;
            return;
        }

        _domainNamespace = $"{kafkaOptions.EnvPrefix}.{CorePlatformNamespace}";
    }

    public Topic Health => new(
        $"{_domainNamespace}.healthchecks",
        null,
        PartitionAmount.Minimal,
        TopicRetentionPeriod.FewMinutes,
        ReplicaFactor.Default // (!) it should be as high as possible
    );

    // ------ events for integration: affiliates, optimove, smartico
    #region integration_events
    public Topic SyncEntityEventsTopic => new(
        $"{_domainNamespace}.SyncEntityEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic OptimoveSyncClientUpdatedEventsTopic => new(
        $"{_domainNamespace}.OptimoveSyncClientUpdatedEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic SmarticoSyncClientUpdatedEventsTopic => new(
        $"{_domainNamespace}.SmarticoSyncClientUpdatedEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic OptimoveSyncClientSegmentsUpdatedEventsTopic => new(
        $"{_domainNamespace}.OptimoveSyncClientSegmentsUpdatedEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic SmarticoSyncClientSegmentsUpdatedEventsTopic => new(
        $"{_domainNamespace}.SmarticoSyncClientSegmentsUpdatedEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );
    #endregion

    public Topic BroadcastEventsTopic => new(
        $"{_domainNamespace}.BroadcastEvents",
        null,
        PartitionAmount.Minimal,
        TopicRetentionPeriod.FewMinutes,
        ReplicaFactor.Default
    );

    public Topic ClientEventsTopic => new(
        $"{_domainNamespace}.ClientEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.ClientEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic ClientSegmentsUpdatedEventsTopic => new(
        $"{_domainNamespace}.ClientSegmentsUpdatedEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic ExpireClientEventsTopic => new(
        $"{_domainNamespace}.ExpireClientEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic ReconsiderDynamicSegmentTopic => new(
        $"{_domainNamespace}.ReconsiderDynamicSegmentEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.ReconsiderDynamicSegmentEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic DynamicSegmentCalculationFinishedTopic => new(
        $"{_domainNamespace}.DynamicSegmentCalculationFinishedEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.DynamicSegmentCalculationFinishedEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic ProcessComplimentaryPointTopic => new(
        $"{_domainNamespace}.ProcessComplimentaryPointEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.ProcessComplimentaryPointEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic UpdateAffiliateStatusTopic => new(
        $"{_domainNamespace}.UpdateAffiliateStatusEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.UpdateAffiliateStatusEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic BonusEvents => new(
        $"{_domainNamespace}.BonusEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.BonusEvents"),
        PartitionAmount.Medium,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic FinalizeWageringEventTopic => new(
        $"{_domainNamespace}.FinalizeWageringEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.FinalizeWageringEvents"),
        PartitionAmount.Medium,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic SettledBetEventTopic => new(
        $"{_domainNamespace}.SettledBetEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.SettledBetEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic BonusRedemptionAttemptEvents => new(
        $"{_domainNamespace}.BonusRedemptionAttemptEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.BonusRedemptionAttemptEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.OneWeek,
        ReplicaFactor.Default
    );

    public Topic UpdatedCacheEvents => new(
        $"{_domainNamespace}.UpdatedCacheEvents",
        null,
        PartitionAmount.Minimal,
        TopicRetentionPeriod.FewMinutes,
        ReplicaFactor.Default
    );

    public Topic BonusActivationEvents => new(
        $"{_domainNamespace}.BonusActivationEventTopic",
        DefaultErrorTopicConfig($"{_domainNamespace}.BonusActivationEventTopic"),
        PartitionAmount.Low,
        TopicRetentionPeriod.OneWeek,
        ReplicaFactor.Default
    );

    private static Topic DefaultErrorTopicConfig(string topicName) => new(
        $"{topicName}.Errors",
        null,
        PartitionAmount.Minimal,
        TopicRetentionPeriod.OneWeek,
        ReplicaFactor.Default
    );

    public Topic BetItemEventTopic => new(
        $"{_domainNamespace}.BetItemEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.BetItemEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic AwardClientBonusEventTopic => new(
        $"{_domainNamespace}.AwardClientBonusEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.AwardClientBonusEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );
    
    public Topic BonusAmountRewardEvents => new(
        $"{_domainNamespace}.BonusAmountRewardEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.BonusAmountRewardEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.OneWeek,
        ReplicaFactor.Default
    );
    
    public Topic ClientBonusChangesEvents => new(
        $"{_domainNamespace}.ClientBonusChangesEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.ClientBonusChangesEvents"),
        PartitionAmount.Medium,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );
    
    public Topic ClientBonusStatusChangedEvents => new(
        $"{_domainNamespace}.ClientBonusStatusChangedEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.ClientBonusStatusChangedEvents"),
        PartitionAmount.Medium,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic OptimoveRealTimeCampaignReceivedEvents => new(
        $"{_domainNamespace}.OptimoveRealTimeCampaignReceivedEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.OptimoveRealTimeCampaignReceivedEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.OneWeek,
        ReplicaFactor.Default
    );

    public Topic OptimoveScheduledCampaignReceivedEvents => new(
        $"{_domainNamespace}.OptimoveScheduledCampaignReceivedEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.OptimoveScheduledCampaignReceivedEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.OneWeek,
        ReplicaFactor.Default
    );

    public Topic OptimoveClientBonusRecommendationsReceivedEvents => new(
        $"{_domainNamespace}.OptimoveClientBonusRecommendationsReceivedEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.OptimoveClientBonusRecommendationsReceivedEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.OneWeek,
        ReplicaFactor.Default
    );

    public Topic UpdatedSegmentTopic => new(
        $"{_domainNamespace}.UpdatedSegmentEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.UpdatedSegmentEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic DeleteSegmentDataTopic => new(
        $"{_domainNamespace}.DeleteSegmentDataEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.DeleteSegmentDataEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic ChangedClientsSegmentTopic => new(
        $"{_domainNamespace}.ChangedClientsSegmentEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.ChangedClientsSegmentEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic ClientClassificationStoreTopic => new(
        $"{_domainNamespace}.ClientClassificationStoreEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.ClientClassificationStoreEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic RafEventsTopic => new(
        $"{_domainNamespace}.RafEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic RafStatisticCalculatedEventsTopic => new(
        $"{_domainNamespace}.RafStatisticCalculatedEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic ClientLoggedInTopic => new(
        $"{_domainNamespace}.ClientLoggedInEvents", 
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic ClientBonusStatisticTopic => new(
        $"{_domainNamespace}.ClientBonusStatisticEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic WagerDebitTransactionEventsTopic => new(
        $"{_domainNamespace}.WagerDebitTransactionEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.WagerDebitTransactionEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic AutoClaimEventsTopic => new(
        $"{_domainNamespace}.AutoClaimEvents",
        DefaultErrorTopicConfig($"{_domainNamespace}.AutoClaimEvents"),
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic FairClientBonusTriggerTopic => new(
        $"{_domainNamespace}.FairClientBonusTriggerEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.TwoWeeks,
        ReplicaFactor.Default
    );

    public Topic ProductsExcludedForClientsTopic => new(
        $"{_domainNamespace}.ProductsExcludedForClientsEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );

    public Topic RafRewardProgramEventsTopic => new(
        $"{_domainNamespace}.RafRewardProgramEvents",
        null,
        PartitionAmount.Low,
        TopicRetentionPeriod.FewDays,
        ReplicaFactor.Default
    );
}