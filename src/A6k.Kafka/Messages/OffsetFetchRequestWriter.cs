using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class OffsetFetchRequestWriter : IMessageWriter<OffsetFetchRequest>
    {
        public void WriteMessage(OffsetFetchRequest message, IBufferWriter<byte> output)
        {
            output.WriteString(message.GroupId);
            output.WriteArray(message.Topics, WriteTopic);
        }

        private void WriteTopic(OffsetFetchRequest.Topic message, IBufferWriter<byte> output)
        {
            output.WriteString(message.Name);
            output.WriteArray(message.PartitionIndexes);
        }
    }
}
