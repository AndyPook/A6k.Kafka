using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class OffsetCommitResponseReader : KafkaResponseReader<OffsetCommitResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out OffsetCommitResponse message)
        {
            message = default;
            if (!reader.TryReadInt(out int throttleTime))
                return false;
            if (!reader.TryReadArray<OffsetCommitResponse.Topic>(TryParseTopic, out var topics))
                return false;

            message = new OffsetCommitResponse(throttleTime,topics);
            return true;
        }

        private bool TryParseTopic(ref SequenceReader<byte> reader, out OffsetCommitResponse.Topic message)
        {
            message = default;
            if (!reader.TryReadString(out string topicName))
                return false;
            if (!reader.TryReadArray<OffsetCommitResponse.Topic.Partition>(TryParsePartition, out var partitions))
                return false;

            message = new OffsetCommitResponse.Topic(topicName,partitions);
            return true;
        }

        private bool TryParsePartition(ref SequenceReader<byte> reader, out OffsetCommitResponse.Topic.Partition message)
        {
            message = default;
            if (!reader.TryReadInt(out var partitonId))
                return false;
            if (!reader.TryReadShort(out var errorCode))
                return false;

            message = new OffsetCommitResponse.Topic.Partition(partitonId,errorCode);
            return true;
        }
    }
}
