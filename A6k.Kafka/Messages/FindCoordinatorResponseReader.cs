using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class FindCoordinatorResponseReader : KafkaResponseReader<FindCoordinatorResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out FindCoordinatorResponse message)
        {
            message = default;
            if (!reader.TryReadBigEndian(out int throttleTime))
                return false;
            if (!reader.TryReadBigEndian(out short errorCode))
                return false;
            if (!reader.TryReadString(out string errorMessage))
                return false;

            if (!reader.TryReadBigEndian(out int nodeId))
                return false;
            if (!reader.TryReadString(out string host))
                return false;
            if (!reader.TryReadBigEndian(out int port))
                return false;

            message = new FindCoordinatorResponse
            (
                throttleTime,
                errorCode,
                errorMessage,
                nodeId,
                host,
                port
            );
            return true;
        }
    }
}
