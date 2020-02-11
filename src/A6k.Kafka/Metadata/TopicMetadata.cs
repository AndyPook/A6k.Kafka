using System.Collections.Generic;

namespace A6k.Kafka.Metadata
{
    public class TopicMetadata
    {
        public TopicMetadata(string topicName, bool isInternal, IReadOnlyList<PartitionMetadata> partitions)
        {
            TopicName = topicName;
            IsInternal = isInternal;
            Partitions = partitions;
        }

        public string TopicName { get; }
        public bool IsInternal { get; }
        public IReadOnlyList<PartitionMetadata> Partitions { get; }
    }
}
