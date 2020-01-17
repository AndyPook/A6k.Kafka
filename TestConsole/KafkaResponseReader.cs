using System;
using System.Buffers;
using Bedrock.Framework.Protocols;

namespace TestConsole
{
    public abstract class KafkaResponseReader<T> : IMessageReader<T>
    {
        protected abstract bool TryParseMessage(ref SequenceReader<byte> reader, out T message);

        public bool TryParseMessage(in ReadOnlySequence<byte> input, ref SequencePosition consumed, ref SequencePosition examined, out T message)
        {
            message = default;
            var reader = new SequenceReader<byte>(input);
            //if (!TryParseHeader(ref reader, out var messageLength, out var correlationId))
            //    return false;
            //if (input.Length < messageLength)
            //    return false;
            //examined = consumed = reader.Position;

            if (!TryParseMessage(ref reader, out message))
                return false;

            examined = consumed = reader.Position;

            return true;
        }

        public bool TryParseHeader(ref SequenceReader<byte> reader, out int messageLength, out int correlationId)
        {
            correlationId = 0;

            if (!reader.TryReadBigEndian(out messageLength))
                return false;

            if (!reader.TryReadBigEndian(out correlationId))
                return false;

            return true;
        }
    }
}
