using System;
using System.Buffers;
using System.Threading.Tasks;

namespace A6k.Kafka
{
    public class DefaultPartitioner : IPartitioner
    {
        private readonly Random random = new Random();

        public async ValueTask<int> GetPartition(string topic, ReadOnlySequence<byte> keyBytes, MetadataManager cluster)
        {
            var t = await cluster.GetTopic(topic);
            int numPartitions = t.Partitions.Count;

            if (keyBytes.Length == 0)
                return random.Next(0, numPartitions);

            // hash the keyBytes to choose a partition
            var partition = Hash.Murmur2.Compute(keyBytes) % numPartitions;
            return (int)partition;
        }

        public void Dispose() { }
    }
}