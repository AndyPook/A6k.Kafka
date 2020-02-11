namespace A6k.Kafka.Messages
{
    /// <summary>
    /// <seealso href="https://kafka.apache.org/protocol#The_Messages_OffsetCommit"/> version 7
    /// </summary>
    public class OffsetCommitRequest
    {
        // default values for the current version
        public const int DEFAULT_GENERATION_ID = -1;
        public static readonly string DEFAULT_MEMBER_ID = string.Empty;
        public const long DEFAULT_RETENTION_TIME = -1L;

        public string GroupId { get; set; }
        public int GenerationId { get; set; }
        public string MemberId { get; set; }
        public string GroupInstanceId { get; set; }
        public Topic[] Topics { get; set; }

        public class Topic
        {
            public string Name { get; set; }
            public Partition[] Partitions { get; set; }

            public class Partition
            {
                public int PartitionId { get; set; }
                public long CommittedOffset { get; set; }
                public int CommittedLeaderEpoc { get; set; }
                public byte[] CommittedMetadata { get; set; }
            }
        }
    }
}
