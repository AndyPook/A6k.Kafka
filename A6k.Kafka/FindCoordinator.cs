using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public class FindCoordinatorRequestWriter : IMessageWriter<(string Key, CoordinatorType KeyType)>
    {
        public void WriteMessage((string Key, CoordinatorType KeyType) message, IBufferWriter<byte> output)
        {
            output.WriteString(message.Key);
            output.WriteByte((byte)message.KeyType);
        }
    }

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
            {
                ThrottleTime = throttleTime,
                ErrorCode = errorCode,
                ErrorMessage = errorMessage,
                NodeId = nodeId,
                Host = host,
                Port = port
            };
            return true;
        }
    }

    public class FindCoordinatorResponse
    {
        public int ThrottleTime { get; set; }
        public short ErrorCode { get; set; }
        public string ErrorMessage { get; set; }
        public int NodeId { get; set; }
        public string Host { get; set; }
        public int Port { get; set; }
    }
}
