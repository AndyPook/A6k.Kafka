using System.Collections.Generic;

namespace A6k.Kafka
{
    //public enum ApiKey : short
    //{
    //    None = -1,
    //    Produce = 0,
    //    Fetch = 1,
    //    Offset = 2,
    //    Metadata = 3,
    //    LeaderAndIsr = 4,
    //    StopReplica = 5,
    //    UpdateMetadata = 6,
    //    ControlledShutdown = 7,
    //    OffsetCommit = 8,
    //    OffsetFetch = 9,
    //    FindCoordinator = 10,
    //    JoinGroup = 11,
    //    Heartbeat = 12,
    //    LeaveGroup = 13,
    //    SyncGroup = 14,
    //    DescribeGroups = 15,
    //    ListGroups = 16,
    //    SaslHandshake = 17,
    //    ApiVersion = 18,
    //    CreateTopics = 19,
    //    DeleteTopics = 20,
    //    DeleteRecords = 21,
    //    InitProducerId = 22,
    //    OffsetForLeaderEpoch = 23,
    //    AddPartitionsToTxn = 24,
    //    AddOffsetsToTxn = 25,
    //    EndTxn = 26,
    //    WriteTxnMarkers = 27,
    //    TxnOffsetCommit = 28,
    //    DescribeAcls = 29,
    //    CreateAcls = 30,
    //    DeleteAcls = 31,
    //    DescribeConfigs = 32,
    //    AlterConfigs = 33,
    //    AlterReplicaLogDirs = 34,
    //    DescribeLogDirs = 35,
    //    SaslAuthenticate = 36,
    //    CreatePartitions = 37,
    //    CreateDelegationToken = 38,
    //    RenewDelegationToken = 39,
    //    ExpireDelegationToken = 40,
    //    DescribeDelegationToken = 41,
    //    DeleteGroups = 42,
    //};

    public static class ApiKey
    {
        // from rd_kafkap_reqhdr in rdkafka_proto.h
        public const short None = -1;
        public const short Produce = 0;
        public const short Fetch = 1;
        public const short Offset = 2;
        public const short Metadata = 3;
        public const short LeaderAndIsr = 4;
        public const short StopReplica = 5;
        public const short UpdateMetadata = 6;
        public const short ControlledShutdown = 7;
        public const short OffsetCommit = 8;
        public const short OffsetFetch = 9;
        public const short FindCoordinator = 10;
        public const short JoinGroup = 11;
        public const short Heartbeat = 12;
        public const short LeaveGroup = 13;
        public const short SyncGroup = 14;
        public const short DescribeGroups = 15;
        public const short ListGroups = 16;
        public const short SaslHandshake = 17;
        public const short ApiVersion = 18;
        public const short CreateTopics = 19;
        public const short DeleteTopics = 20;
        public const short DeleteRecords = 21;
        public const short InitProducerId = 22;
        public const short OffsetForLeaderEpoch = 23;
        public const short AddPartitionsToTxn = 24;
        public const short AddOffsetsToTxn = 25;
        public const short EndTxn = 26;
        public const short WriteTxnMarkers = 27;
        public const short TxnOffsetCommit = 28;
        public const short DescribeAcls = 29;
        public const short CreateAcls = 30;
        public const short DeleteAcls = 31;
        public const short DescribeConfigs = 32;
        public const short AlterConfigs = 33;
        public const short AlterReplicaLogDirs = 34;
        public const short DescribeLogDirs = 35;
        public const short SaslAuthenticate = 36;
        public const short CreatePartitions = 37;
        public const short CreateDelegationToken = 38;
        public const short RenewDelegationToken = 39;
        public const short ExpireDelegationToken = 40;
        public const short DescribeDelegationToken = 41;
        public const short DeleteGroups = 42;

        // from rd_kafka_ApiKey2str in rdkafka_proto.h
        static Dictionary<short, string> names = new Dictionary<short, string>
        {
            [Produce] = "Produce",
            [Fetch] = "Fetch",
            [Offset] = "Offset",
            [Metadata] = "Metadata",
            [LeaderAndIsr] = "LeaderAndIsr",
            [StopReplica] = "StopReplica",
            [UpdateMetadata] = "UpdateMetadata",
            [ControlledShutdown] = "ControlledShutdown",
            [OffsetCommit] = "OffsetCommit",
            [OffsetFetch] = "OffsetFetch",
            [FindCoordinator] = "FindCoordinator",
            [JoinGroup] = "JoinGroup",
            [Heartbeat] = "Heartbeat",
            [LeaveGroup] = "LeaveGroup",
            [SyncGroup] = "SyncGroup",
            [DescribeGroups] = "DescribeGroups",
            [ListGroups] = "ListGroups",
            [SaslHandshake] = "SaslHandshake",
            [ApiVersion] = "ApiVersion",
            [CreateTopics] = "CreateTopics",
            [DeleteTopics] = "DeleteTopics",
            [DeleteRecords] = "DeleteRecords",
            [InitProducerId] = "InitProducerId",
            [OffsetForLeaderEpoch] = "OffsetForLeaderEpoch",
            [AddPartitionsToTxn] = "AddPartitionsToTxn",
            [AddOffsetsToTxn] = "AddOffsetsToTxn",
            [EndTxn] = "EndTxn",
            [WriteTxnMarkers] = "WriteTxnMarkers",
            [TxnOffsetCommit] = "TxnOffsetCommit",
            [DescribeAcls] = "DescribeAcls",
            [CreateAcls] = "CreateAcls",
            [DeleteAcls] = "DeleteAcls",
            [DescribeConfigs] = "DescribeConfigs",
            [AlterConfigs] = "AlterConfigs",
            [AlterReplicaLogDirs] = "AlterReplicaLogDirs",
            [DescribeLogDirs] = "DescribeLogDirs",
            [SaslAuthenticate] = "SaslAuthenticate",
            [CreatePartitions] = "CreatePartitions",
            [CreateDelegationToken] = "CreateDelegationToken",
            [RenewDelegationToken] = "RenewDelegationToken",
            [ExpireDelegationToken] = "ExpireDelegationToken",
            [DescribeDelegationToken] = "DescribeDelegationToken",
            [DeleteGroups] = "DeleteGroups"
        };

        public static string GetName(short apikey)
        {
            if (names.TryGetValue(apikey, out var name))
                return name;
            return apikey.ToString();
        }
    }
}
