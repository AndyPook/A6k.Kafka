using System;
using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public class KafkaResponseHeaderReader : IMessageReader<(int MessageLength, int CorrelationId)>
    {
        public bool TryParseMessage(in ReadOnlySequence<byte> input, ref SequencePosition consumed, ref SequencePosition examined, out (int MessageLength, int CorrelationId) message)
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
            message = default;
            var reader = new SequenceReader<byte>(input);

            if (!reader.TryReadBigEndian(out int messageLength))
                return false;

            if (!reader.TryReadBigEndian(out int correlationId))
                return false;

            message = (messageLength, correlationId);
            examined = consumed = reader.Position;
            return true;
        }
    }
}
