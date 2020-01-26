namespace A6k.Kafka.Messages
{
    /// <summary>
    /// <seealso href="https://kafka.apache.org/protocol#The_Messages_OffsetCommit"/> version 7
    /// </summary>
    public class OffsetCommitRequest
    {
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
