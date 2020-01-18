using System;
using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public abstract class KafkaRequest<T> : IMessageWriter<T>
    {
        public abstract void WriteMessage(T message, IBufferWriter<byte> output);
    }

    public abstract class KafkaResponse<T> : IMessageReader<T>
    {
        public abstract bool TryParseMessage(in ReadOnlySequence<byte> input, ref SequencePosition consumed, ref SequencePosition examined, out T message);
    }
}
