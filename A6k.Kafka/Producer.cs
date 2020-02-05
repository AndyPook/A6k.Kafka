using System;
using System.Threading.Tasks;
using A6k.Kafka.Messages;

namespace A6k.Kafka
{
    public static class ProducerExtensions
    {
        public static ValueTask<ProduceResponse> Produce<TKey, TValue>(this Producer<TKey, TValue> producer, TKey key, TValue value)
        {
            var message = new Message<TKey, TValue>
            {
                Key = key,
                Value = value
            };

            return producer.Produce(message);
        }
    }

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

            if (keySerializer == null && !IntrinsicSerializers.TryGetSerializer(out keySerializer))
                throw new ArgumentException($"{nameof(keySerializer)} not provided or discoverable");
            else
                this.keySerializer = keySerializer;

            if (valueSerializer == null && !IntrinsicSerializers.TryGetSerializer(out valueSerializer))
                throw new ArgumentException($"{nameof(valueSerializer)} not provided or discoverable");
            else
                this.valueSerializer = valueSerializer;
        }

        public async ValueTask<ProduceResponse> Produce(Message<TKey, TValue> message)
        {
            var record = ProducerRecord.Create(message, keySerializer, valueSerializer);
            record.Topic = topic;

            var t = await cluster.GetTopic(record.Topic);
            if (message.PartitionId.HasValue)
                record.PartitionId = message.PartitionId.Value;
            else if (t.Partitions.Count == 1)
                record.PartitionId = 0;
            else
                record.PartitionId = await partitioner.GetPartition(t, record.KeyBytes);

            if (!record.PartitionId.HasValue)
                throw new InvalidOperationException("PartitionId not specified");

            var partitionLeader = t.Partitions[record.PartitionId.Value].Leader;
            var b = cluster.GetBroker(partitionLeader);

            var response = await b.Connection.Produce(record);
            if (response.Topics.Count > 1 || response.Topics[0].Partitions.Count > 1)
                throw new KafkaException("Expected single partition in Produce request");

            var partitionResponse = response.Topics[0].Partitions[0];
            switch (partitionResponse.ErrorCode)
            {
                case ResponseError.LEADER_NOT_AVAILABLE:
                case ResponseError.NOT_LEADER_FOR_PARTITION:
                case ResponseError.PREFERRED_LEADER_NOT_AVAILABLE:
                    throw new KafkaException("oops");
            }

            return response;
        }
    }
}
