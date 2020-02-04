using System;
using System.Buffers;
using System.Threading.Tasks;

namespace A6k.Kafka
{
    public interface IPartitioner : IDisposable
    {
        /**
         * Compute the partition for the given record.
         *
         * @param topic The topic name
         * @param key The key to partition on (or null if no key)
         * @param keyBytes The serialized key to partition on( or null if no key)
         * @param value The value to partition on or null
         * @param valueBytes The serialized value to partition on or null
         * @param cluster The current cluster metadata
         */
        ValueTask<int> GetPartition(string topic, ReadOnlySequence<byte> keyBytes, MetadataManager cluster);
    }
}