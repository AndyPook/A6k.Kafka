using System;
using System.Buffers;

namespace A6k.Kafka
{
    public class ApiVersionResponseReader : KafkaResponseReader<ApiVersionResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out ApiVersionResponse message)
        {
            message = new ApiVersionResponse();
            if (!reader.TryReadBigEndian(out short errorCode))
                return false;
            if(errorCode>0)
            {
                message.ErrorCode = errorCode;
                return true;
            }
            if (!reader.TryReadBigEndian(out int count))
                return false;

            message.ApiVersions = new ApiVersionResponse.ApiVersion[count];
            for(int i = 0; i < count; i++)
            {
                if (!reader.TryReadBigEndian(out short apikey))
                    return false;
                if (!reader.TryReadBigEndian(out short min))
                    return false;
                if (!reader.TryReadBigEndian(out short max))
                    return false;
                message.ApiVersions[i] = new ApiVersionResponse.ApiVersion
                {
                    ApiKey = apikey,
                    MinVersion = min,
                    MaxVersion = max
                };
            }

            return true;
        }
    }
}
