using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class HeartbeatResponseReader : KafkaResponseReader<HeartbeatResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out HeartbeatResponse message)
        {
            message = default;
            if (!reader.TryReadInt(out var throttleTime))
                return false;
            if (!reader.TryReadShort(out short errorCode))
                return false;

            message = new HeartbeatResponse(throttleTime, errorCode);
            return true;
        }
    }
}
