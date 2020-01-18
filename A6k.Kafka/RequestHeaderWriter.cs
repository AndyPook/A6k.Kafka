using System.Buffers;
using Bedrock.Framework.Protocols;

namespace A6k.Kafka
{
    //public class KafkaResponseReader : IMessageReader<ResponseMessage>
    //{
    //    public bool TryParseMessage(in ReadOnlySequence<byte> input, ref SequencePosition consumed, ref SequencePosition examined, out ResponseMessage message)
    //    {
    //        var reader = new SequenceReader<byte>(input);
    //        if (!reader.TryReadBigEndian(out int correlationId))
    //        {
    //            message = default;
    //            return false;
    //        }

    //        //var payload = input.Slice(reader.Position, length);
    //        //message = new Message(payload);

    //        consumed = payload.End;
    //        examined = consumed;
    //        return true;
    //    }
    //}

    //public class KafkaRequestMessageWriter : IMessageWriter<RequestMessage>
    //{
    //    public void WriteMessage(RequestMessage message, IBufferWriter<byte> output)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
    public class RequestHeaderWriter : IMessageWriter<RequestHeaderV1>
    {
        public void WriteMessage(RequestHeaderV1 message, IBufferWriter<byte> output)
        {
            output.Write(message.ApiKey);
            output.Write(message.ApiVersion);
            output.Write(message.CorrelationId);
            output.Write(message.ClientId);
        }
    }

}
