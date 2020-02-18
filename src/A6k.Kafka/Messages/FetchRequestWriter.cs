using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public abstract class KafkaMessageWriter<T> : IMessageWriter<T>
    {
        public void WriteMessage(T message, IBufferWriter<byte> output)
        {
            WriteMessage(message, new KafkaWriter(output));
        }

        protected abstract void WriteMessage(T message, KafkaWriter output);
    }

    public class FetchRequestWriter2 : KafkaMessageWriter<FetchRequest>
    {
        protected override void WriteMessage(FetchRequest message, KafkaWriter output)
        {
            output.WriteInt(message.ReplicaId);
            output.WriteInt(message.MaxWaitTime);
            output.WriteInt(message.MinBytes);
            output.WriteInt(message.MaxBytes);
            output.WriteByte(message.IsolationLevel);
            output.WriteInt(message.SessionId);
            output.WriteInt(message.SessionEpoc);
            output.WriteArray(message.Topics, WriteTopic);
            output.WriteArray(message.ForgottenTopics, WriteForgottenTopic);
            output.WriteString(message.Rack);
        }

        public void WriteTopic(FetchRequest.Topic message, KafkaWriter output)
        {
            output.WriteString(message.TopicName);
            output.WriteArray(message.Partitions, WriteTopicPartition);
        }

        public void WriteTopicPartition(FetchRequest.Topic.Partition message, KafkaWriter output)
        {
            output.WriteInt(message.PartitionId);
            output.WriteInt(message.CurrentLeaderEpoc);
            output.WriteLong(message.FetchOffset);
            output.WriteLong(message.LogStartOffset);
            output.WriteInt(message.PartitionMaxBytes);
        }

        public void WriteForgottenTopic(FetchRequest.ForgottenTopicsData message, KafkaWriter output)
        {
            output.WriteString(message.TopicName);
            output.WriteArray(message.Partitions, (x, o) => o.WriteInt(x));
        }
    }

    public class FetchRequestWriter : IMessageWriter<FetchRequest>
    {
        public void WriteMessage(FetchRequest message, IBufferWriter<byte> output)
        {
            output.WriteInt(message.ReplicaId);
            output.WriteInt(message.MaxWaitTime);
            output.WriteInt(message.MinBytes);
            output.WriteInt(message.MaxBytes);
            output.WriteByte(message.IsolationLevel);
            output.WriteInt(message.SessionId);
            output.WriteInt(message.SessionEpoc);
            output.WriteArray(message.Topics, WriteTopic);
            output.WriteArray(message.ForgottenTopics, WriteForgottenTopic);
            output.WriteString(message.Rack);
        }

        public void WriteTopic(FetchRequest.Topic message, IBufferWriter<byte> output)
        {
            output.WriteString(message.TopicName);
            output.WriteArray(message.Partitions, WriteTopicPartition);
        }

        public void WriteTopicPartition(FetchRequest.Topic.Partition message, IBufferWriter<byte> output)
        {
            output.WriteInt(message.PartitionId);
            output.WriteInt(message.CurrentLeaderEpoc);
            output.WriteLong(message.FetchOffset);
            output.WriteLong(message.LogStartOffset);
            output.WriteInt(message.PartitionMaxBytes);
        }

        public void WriteForgottenTopic(FetchRequest.ForgottenTopicsData message, IBufferWriter<byte> output)
        {
            output.WriteString(message.TopicName);
            output.WriteArray(message.Partitions, (x, o) => o.WriteInt(x));
        }
    }
}
