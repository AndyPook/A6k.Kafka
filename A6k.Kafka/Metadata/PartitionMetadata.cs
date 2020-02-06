using System.Collections.Generic;

namespace A6k.Kafka.Metadata
{
    public class PartitionMetadata
    {
        public PartitionMetadata(int partitionId, int leader, int[] replicas, int[] isr)
        {
            PartitionId = partitionId;
            Leader = leader;
            Replicas = replicas;
            Isr = isr;
        }

        public int PartitionId { get; }
        public int Leader { get; }
        public IReadOnlyList<int> Replicas { get; }
        public IReadOnlyList<int> Isr { get; }
    }
}
