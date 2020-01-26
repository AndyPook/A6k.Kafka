using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class HeartbeatRequestWriter : IMessageWriter<HeartbeatRequest>
    {
        public void WriteMessage(HeartbeatRequest message, IBufferWriter<byte> output)
        {
            output.WriteString(message.GroupId);
            output.WriteInt(message.GenerationId);
            output.WriteString(message.MemberId);
        }
    }
}
