using System;
using System.Buffers;
using System.Threading.Tasks;
using A6k.Kafka.Metadata;

namespace A6k.Kafka
{
    public class DefaultPartitioner : IPartitioner
    {
        private readonly Random random = new Random();

        public ValueTask<int> GetPartition(TopicMetadata topic, ReadOnlySequence<byte> keyBytes)
        {
            int numPartitions = topic.Partitions.Count;
            int partition;

            if (topic.Partitions.Count == 1)
                partition = 0; 
            else if (keyBytes.Length == 0)
                partition = random.Next(0, numPartitions);
            else
                // hash the keyBytes to choose a partition
                partition = (int)(Hash.Murmur2.Compute(keyBytes) % numPartitions);

            return new ValueTask<int>(partition);
        }

        public void Dispose() { }
    }
}