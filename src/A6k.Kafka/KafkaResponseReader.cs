using System;
using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    public abstract class KafkaResponseReader<T> : IMessageReader<T>
    {
        protected abstract bool TryParseMessage(ref SequenceReader<byte> reader, out T message);

        public bool TryParseMessage(in ReadOnlySequence<byte> input, ref SequencePosition consumed, ref SequencePosition examined, out T message)
        {
            var reader = new SequenceReader<byte>(input);
            if (!TryParseMessage(ref reader, out message))
                return false;

            examined = consumed = reader.Position;

            return true;
        }
    }
}
