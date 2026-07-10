namespace Tech.Deployment.KafkaSetup.Constants;

public static class Constants
{
    public static class KafkaConfig
    {
        public const string RetentionMs = "retention.ms";
        public const string MinInSyncReplicas = "min.insync.replicas";
        public const string UncleanLeaderElectionEnable = "unclean.leader.election.enable";
        public const string MessageFormatVersion = "message.format.version";
    }
}