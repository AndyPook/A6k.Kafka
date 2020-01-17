using System.Buffers;

namespace TestConsole
{
    internal static class KafkaResponseReader_
    {
        /* https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Responses
            +-----------------------------------------------+
            |                 Length (32)                   |
            +---------------+---------------+---------------+
            |   CorrelationId (32) |
            +=+=============================================================+
            |                   Response Payload (0...)                   ...
            +---------------------------------------------------------------+
        */

        public static bool TryReadResponse(ref ReadOnlySequence<byte> buffer, out int correlationId, out ReadOnlySequence<byte> framePayload)
        {
            framePayload = ReadOnlySequence<byte>.Empty;
            correlationId = 0;

            if (buffer.Length < 8)
                return false;

            var reader = new SequenceReader<byte>(buffer);
            if (!reader.TryReadBigEndian(out int messageLength))
                return false;

            if (!reader.TryReadBigEndian(out correlationId))
                return false;

            if (buffer.Length < messageLength)
                return false;

            // The remaining payload minus the extra fields
            framePayload = buffer.Slice(8, messageLength - 8);
            buffer = buffer.Slice(framePayload.End);

            return true;
        }
    }
}
