namespace A6k.Kafka.Messages
{
    /// <summary>
    /// <seealso href="https://kafka.apache.org/protocol#The_Messages_OffsetCommit"/> version 1-5
    /// </summary>
    public class OffsetFetchRequest
    {
        public string GroupId { get; set; }
        public Topic[] Topics { get; set; }

        public class Topic
        {
            public string Name { get; set; }
            public int[] PartitionIndexes { get; set; }
        }
    }
}
