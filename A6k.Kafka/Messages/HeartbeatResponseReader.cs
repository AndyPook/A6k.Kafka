using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class HeartbeatResponseReader : KafkaResponseReader<HeartbeatResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out HeartbeatResponse message)
        {
            message = default;
            if (!reader.TryReadShort(out short errorCode))
                return false;

            message = new HeartbeatResponse(errorCode);
            return true;
        }
    }
}
