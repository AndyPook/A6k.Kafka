using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class OffsetFetchResponse
    {
        public IReadOnlyCollection<Topic> Topics { get; set; }

        public class Topic
        {
            public string Name { get; set; }
            public Partition[] Partitions { get; set; }

            public class Partition
            {
                public int PartitionId { get; set; }
                public long CommittedOffset { get; set; }
                public string Metadata { get; set; }
                public ResponseError ErrorCode { get; set; }
            }
        }
    }
}
