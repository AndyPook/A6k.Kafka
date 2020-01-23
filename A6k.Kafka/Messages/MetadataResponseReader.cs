using System;
using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class MetadataResponseReader : KafkaResponseReader<MetadataResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out MetadataResponse message)
        {
            message = default;
            if (!reader.TryReadArray<MetadataResponse.Broker>(TryParseBroker, out var brokers))
                return false;
            if (!reader.TryReadString(out var clusterid))
                return false;
            if (!reader.TryReadBigEndian(out int controllerid))
                return false;
            if (!reader.TryReadArray<MetadataResponse.TopicMetadata>(TryParseTopic, out var topics))
                return false;

            message = new MetadataResponse(brokers, clusterid, controllerid, topics);
            return true;
        }

        private bool TryParseBroker(ref SequenceReader<byte> reader, out MetadataResponse.Broker broker)
        {
            broker = default;
            if (!reader.TryReadBigEndian(out int nodeid))
                return false;
            if (!reader.TryReadString(out string host))
                return false;
            if (!reader.TryReadBigEndian(out int port))
                return false;
            if (!reader.TryReadString(out string rack))
                return false;

            broker = new MetadataResponse.Broker(nodeid, host, port, rack);
            return true;
        }
        private bool TryParseTopic(ref SequenceReader<byte> reader, out MetadataResponse.TopicMetadata topic)
        {
            topic = default;
            if (!reader.TryReadBigEndian(out short errorcode))
                return false;
            if (errorcode > 0)
            {
                topic = new MetadataResponse.TopicMetadata(errorcode);
                return true;
            }

            if (!reader.TryReadString(out string topicName))
                return false;
            if (!reader.TryReadBool(out var isInternal))
                return false;

            if (!reader.TryReadArray<MetadataResponse.PartitionMetadata>(TryParsePartition, out var partitions))
                return false;

            topic = new MetadataResponse.TopicMetadata(topicName, isInternal, partitions);
            return true;
        }

        private bool TryParsePartition(ref SequenceReader<byte> reader, out MetadataResponse.PartitionMetadata partition)
        {
            partition = default;
            if (!reader.TryReadBigEndian(out short errorcode))
                return false;
            if (errorcode > 0)
            {
                partition = new MetadataResponse.PartitionMetadata(errorcode);
                return true;
            }

            if (!reader.TryReadBigEndian(out int partitionid))
                return false;
            if (!reader.TryReadBigEndian(out int leader))
                return false;
            if (!reader.TryReadArrayOfInt(out var replicas))
                return false;
            if (!reader.TryReadArrayOfInt(out var isr))
                return false;

            partition = new MetadataResponse.PartitionMetadata(partitionid, leader, replicas, isr);
            return true;
        }
    }
}
