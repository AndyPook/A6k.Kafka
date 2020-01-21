using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public class RequestHeaderWriter : IMessageWriter<RequestHeaderV1>
    {
        public void WriteMessage(RequestHeaderV1 message, IBufferWriter<byte> output)
        {
            output.WriteShort(message.ApiKey);
            output.WriteShort(message.ApiVersion);
            output.WriteInt(message.CorrelationId);
            output.WriteString(message.ClientId);
        }
    }
}
