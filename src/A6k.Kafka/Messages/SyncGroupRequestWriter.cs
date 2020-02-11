using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class SyncGroupRequestWriter : IMessageWriter<SyncGroupRequest>
    {
        public void WriteMessage(SyncGroupRequest message, IBufferWriter<byte> output)
        {
            output.WriteString(message.GroupId);
            output.WriteInt(message.GenerationId);
            output.WriteString(message.MemberId);
            output.WriteNullableString(message.GroupInstanceId);
            output.WriteArray(message.Assignments, WriteAssignment);
        }

        private void WriteAssignment(SyncGroupRequest.Assignment message, IBufferWriter<byte> output)
        {
            output.WriteString(message.MemberId);
            output.WriteBytes(message.AssignmentData);
        }
    }
}
