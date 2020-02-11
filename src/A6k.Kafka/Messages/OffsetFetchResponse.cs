using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class OffsetFetchResponse
    {
        public OffsetFetchResponse(Topic[] topics)
        {
            Topics = topics;
        }

        public IReadOnlyList<Topic> Topics { get; }

        public class Topic
        {
            public Topic(string name, Partition[] partitions)
            {
                Name = name;
                Partitions = partitions;
            }

            public string Name { get; }
            public IReadOnlyList<Partition> Partitions { get; }

            public class Partition
            {
                public Partition(int partitionId, long committedOffset, string metadata, ResponseError errorCode)
                {
                    PartitionId = partitionId;
                    CommittedOffset = committedOffset;
                    Metadata = metadata;
                    ErrorCode = errorCode;
                }

                public int PartitionId { get; }
                public long CommittedOffset { get; }
                public string Metadata { get; }
                public ResponseError ErrorCode { get; }
            }
        }
    }
}
