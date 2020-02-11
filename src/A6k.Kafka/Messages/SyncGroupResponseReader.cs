using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class SyncGroupResponseReader : KafkaResponseReader<SyncGroupResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out SyncGroupResponse message)
        {
            message = default;
            if (!reader.TryReadInt(out var throttleTime))
                return false;
            if (!reader.TryReadShort(out var errorCode))
                return false;
            if (!reader.TryReadBytes(out var assignment))
                return false;

            message = new SyncGroupResponse(throttleTime, errorCode, assignment.ToArray());
            return true;
        }
    }
}
