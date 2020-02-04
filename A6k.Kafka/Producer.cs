using System;
using System.Threading.Tasks;
using A6k.Kafka.Messages;

namespace A6k.Kafka
{
    public class Producer<TKey, TValue>
    {
        private readonly string topic;
        private readonly MetadataManager cluster;

        private IPartitioner partitioner = new DefaultPartitioner();
        private ISerializer<TKey> keySerializer;
        private ISerializer<TValue> valueSerializer;

        public Producer(string topic, MetadataManager cluster, ISerializer<TKey> keySerializer = null, ISerializer<TValue> valueSerializer = null)
        {
            this.topic = topic;
            this.cluster = cluster;

            if (keySerializer == null && !IntrinsicWriter.TryGetSerializer(out keySerializer))
                throw new ArgumentException($"{nameof(keySerializer)} not provided or discoverable");
            else
                this.keySerializer = keySerializer;

            if (valueSerializer == null && !IntrinsicWriter.TryGetSerializer(out valueSerializer))
                throw new ArgumentException($"{nameof(valueSerializer)} not provided or discoverable");
            else
                this.valueSerializer = valueSerializer;
        }

        public async ValueTask Produce(Message<TKey, TValue> message)
        {
            var record = ProducerRecord.Create(message, keySerializer, valueSerializer);

            var t = await cluster.GetTopic(topic);
            if (message.PartitionId.HasValue)
                record.PartitionId = message.PartitionId.Value;
            else
            {
                if (t.Partitions.Count == 0)
                    record.PartitionId = 0;
                else
                    record.PartitionId = await partitioner.GetPartition(topic, record.KeyBytes, cluster);
            }

            if (!record.PartitionId.HasValue)
                throw new InvalidOperationException("PartitionId not specified");

            var partitionLeader = t.Partitions[record.PartitionId.Value].Leader;
            var b = cluster.GetBroker(partitionLeader);

            var response = await b.Connection.Produce(topic, message, keySerializer, valueSerializer);
            // do something with ErrorCode
        }
    }
}
