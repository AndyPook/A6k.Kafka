using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class ProduceResponseReader : KafkaResponseReader<ProduceResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out ProduceResponse message)
        {
            message = default;

            if (!reader.TryReadArray<ProduceResponse.Response>(TryParseResponse, out var responses))
                return false;
            if (!reader.TryReadInt(out var throttleTime))
                return false;

            message = new ProduceResponse(responses, throttleTime);
            return true;
        }

        private bool TryParseResponse(ref SequenceReader<byte> reader, out ProduceResponse.Response response)
        {
            response = default;
            if (!reader.TryReadString(out var topic))
                return false;
            if (!reader.TryReadArray<ProduceResponse.Response.PartitionResponse>(TryParsePartition, out var partitions))
                return false;

            response = new ProduceResponse.Response(topic, partitions);
            return true;
        }

        private bool TryParsePartition(ref SequenceReader<byte> reader, out ProduceResponse.Response.PartitionResponse partition)
        {
            partition = default;
            if (!reader.TryReadBigEndian(out int partitionId))
                return false;
            if (!reader.TryReadBigEndian(out short error))
                return false;
            if (!reader.TryReadBigEndian(out short baseOffset))
                return false;
            if (!reader.TryReadBigEndian(out long logAppendTime))
                return false;
            if (!reader.TryReadBigEndian(out long logStartOffset))
                return false;
            //if (!reader.TryReadArray<ProduceResponse.PartitionResponse.RecordError>(TryParsePartitionError, out var errors))
            //    return false;

            partition = new ProduceResponse.Response.PartitionResponse(partitionId, error, baseOffset, logAppendTime, logStartOffset); //, errors);
            return true;
        }

        //private bool TryParsePartitionError(ref SequenceReader<byte> reader, out ProduceResponse.PartitionResponse.RecordError error)
        //{
        //    error = default;
        //    if (!reader.TryReadBigEndian(out int errorIndex))
        //        return false;
        //    if (!reader.TryReadString(out var msg))
        //        return false;

        //    error = new ProduceResponse.PartitionResponse.RecordError(errorIndex, msg);
        //    return true;
        //}
    }
}
