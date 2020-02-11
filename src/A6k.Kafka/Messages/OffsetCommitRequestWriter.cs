using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class OffsetCommitRequestWriter : IMessageWriter<OffsetCommitRequest>
    {
        public void WriteMessage(OffsetCommitRequest message, IBufferWriter<byte> output)
        {
            output.WriteString(message.GroupId);
            output.WriteInt(message.GenerationId);
            output.WriteString(message.MemberId);
            output.WriteString(message.GroupInstanceId);
            output.WriteArray(message.Topics, WriteTopic);
        }

        private void WriteTopic(OffsetCommitRequest.Topic message, IBufferWriter<byte> output)
        {
            output.WriteString(message.Name);
            output.WriteArray(message.Partitions, WritePartition);
        }

        private void WritePartition(OffsetCommitRequest.Topic.Partition message, IBufferWriter<byte> output)
        {
            output.WriteInt(message.PartitionId);
            output.WriteLong(message.CommittedOffset);
            output.WriteInt(message.CommittedLeaderEpoc);
            output.Write(message.CommittedMetadata);
        }
    }
}
