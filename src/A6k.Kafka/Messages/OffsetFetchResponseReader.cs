using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class OffsetFetchResponseReader : KafkaResponseReader<OffsetFetchResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out OffsetFetchResponse message)
        {
            message = default;
            if (!reader.TryReadArray<OffsetFetchResponse.Topic>(TryParseTopic, out var topics))
                return false;

            message = new OffsetFetchResponse(topics);
            return true;
        }

        private bool TryParseTopic(ref SequenceReader<byte> reader, out OffsetFetchResponse.Topic message)
        {
            message = default;
            if (!reader.TryReadString(out var topicName))
                return false;
            if (!reader.TryReadArray<OffsetFetchResponse.Topic.Partition>(TryParsePartition, out var partitions))
                return false;

            message = new OffsetFetchResponse.Topic(topicName,partitions);
            return true;
        }

        private bool TryParsePartition(ref SequenceReader<byte> reader, out OffsetFetchResponse.Topic.Partition message)
        {
            message = default;
            if (!reader.TryReadInt(out var partitionId))
                return false;
            if (!reader.TryReadLong(out var committedOffset))
                return false;
            if (!reader.TryReadString(out var metadata))
                return false;
            if (!reader.TryReadShort(out var errorCode))
                return false;

            message = new OffsetFetchResponse.Topic.Partition
            (
                partitionId,
                committedOffset,
                metadata,
                (ResponseError)errorCode
            );
            return true;
        }
    }
}
