using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class OffsetCommitResponse
    {
        public int ThrottleTime { get; set; }
        public IReadOnlyCollection<Topic> Topics { get; set; }

        public class Topic
        {
            public string Name { get; set; }
            public IReadOnlyCollection<Partition> Partitions { get; set; }

            public class Partition
            {
                public int PartitionIndex { get; set; }
                public ResponseError ErrorCode { get; set; }
            }
        }
    }
}
