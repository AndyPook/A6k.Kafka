using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class FindCoordinatorRequestWriter : IMessageWriter<(string Key, CoordinatorType KeyType)>
    {
        public void WriteMessage((string Key, CoordinatorType KeyType) message, IBufferWriter<byte> output)
        {
            output.WriteString(message.Key);
            output.WriteByte((byte)message.KeyType);
        }
    }
}
