using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class JoinGroupRequestWriter : IMessageWriter<JoinGroupRequest>
    {
        public void WriteMessage(JoinGroupRequest message, IBufferWriter<byte> output)
        {
            output.WriteString(message.GroupId);
            output.WriteInt(message.SessionTimeout);
            output.WriteInt(message.RebalanceTimeout);
            output.WriteString(message.MemberId);
            //output.WriteNullableString(message.GroupInstanceId);
            output.WriteString(message.ProtocolType);
            output.WriteArray(message.Protocols, WriteProtocol);
        }

        private void WriteProtocol(JoinGroupRequest.Protocol message, IBufferWriter<byte> output)
        {
            output.WriteString(message.Name);
            output.WriteArray(message.Metadata);
        }
    }
}
