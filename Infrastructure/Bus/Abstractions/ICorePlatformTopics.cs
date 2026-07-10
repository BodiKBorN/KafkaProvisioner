using Tech.Kafka.Clients.Admin;

namespace Infrastructure.Bus.Abstractions;

public interface ICorePlatformTopics
{
    public Topic Health { get; }

    // events for integration: affiliates, optimove, smartico
    #region integration_events
    Topic SyncEntityEventsTopic { get; }
    Topic OptimoveSyncClientUpdatedEventsTopic { get; }
    Topic SmarticoSyncClientUpdatedEventsTopic { get; }
    Topic OptimoveSyncClientSegmentsUpdatedEventsTopic { get; }
    Topic SmarticoSyncClientSegmentsUpdatedEventsTopic { get; }
    #endregion

    Topic BroadcastEventsTopic { get; }
    Topic ClientEventsTopic { get; }
    Topic ClientSegmentsUpdatedEventsTopic { get; }
    Topic ExpireClientEventsTopic { get; }
    Topic ReconsiderDynamicSegmentTopic { get; }
    Topic DynamicSegmentCalculationFinishedTopic { get; }
    Topic ProcessComplimentaryPointTopic { get; }
    Topic UpdateAffiliateStatusTopic { get; }
    Topic BonusEvents { get; }
    Topic FinalizeWageringEventTopic { get; }
    Topic SettledBetEventTopic { get; }
    Topic BonusRedemptionAttemptEvents { get; }
    Topic BonusActivationEvents { get; }
    Topic UpdatedCacheEvents { get; }
    Topic BetItemEventTopic { get; }
    Topic AwardClientBonusEventTopic { get; }
    Topic ClientBonusChangesEvents { get; }
    Topic ClientBonusStatusChangedEvents { get; }
    Topic BonusAmountRewardEvents { get; }
    Topic OptimoveRealTimeCampaignReceivedEvents { get; }
    Topic OptimoveScheduledCampaignReceivedEvents { get; }
    Topic OptimoveClientBonusRecommendationsReceivedEvents { get; }
    Topic UpdatedSegmentTopic { get; }
    Topic DeleteSegmentDataTopic { get; }
    Topic ChangedClientsSegmentTopic { get; }
    Topic ClientClassificationStoreTopic { get; }
    Topic RafEventsTopic { get; }
    Topic RafStatisticCalculatedEventsTopic { get; }
    Topic ClientLoggedInTopic { get; }
    Topic ClientBonusStatisticTopic { get; }
    Topic WagerDebitTransactionEventsTopic { get; }
    Topic AutoClaimEventsTopic { get; }
    Topic FairClientBonusTriggerTopic { get; }
    Topic ProductsExcludedForClientsTopic { get; }
    Topic RafRewardProgramEventsTopic { get; }
}