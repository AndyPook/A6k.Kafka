using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class OffsetCommitResponse
    {
        public OffsetCommitResponse(int throttleTime, Topic[] topics)
        {
            ThrottleTime = throttleTime;
            Topics = topics;
        }

        public int ThrottleTime { get; }
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
                public Partition(int partitionIndex, short errorCode)
                {
                    PartitionIndex = partitionIndex;
                    ErrorCode = (ResponseError)errorCode;
                }

                public int PartitionIndex { get; }
                public ResponseError ErrorCode { get; }
            }
        }
    }
}
