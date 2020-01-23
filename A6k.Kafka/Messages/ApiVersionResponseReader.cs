using System;
using System.Buffers;

namespace A6k.Kafka.Messages
{
    public class ApiVersionResponseReader : KafkaResponseReader<ApiVersionResponse>
    {
        protected override bool TryParseMessage(ref SequenceReader<byte> reader, out ApiVersionResponse message)
        {
            message = default;
            if (!reader.TryReadBigEndian(out short errorCode))
                return false;
            if(errorCode>0)
            {
                message = new ApiVersionResponse(errorCode, new ApiVersionResponse.ApiVersion[0]);
                return true;
            }
            if (!reader.TryReadBigEndian(out int count))
                return false;

            var apiVersions = new ApiVersionResponse.ApiVersion[count];
            for(int i = 0; i < count; i++)
            {
                if (!reader.TryReadBigEndian(out short apikey))
                    return false;
                if (!reader.TryReadBigEndian(out short min))
                    return false;
                if (!reader.TryReadBigEndian(out short max))
                    return false;
                apiVersions[i] = new ApiVersionResponse.ApiVersion(apikey, min, max);
            }

            message = new ApiVersionResponse(errorCode, apiVersions);
            return true;
        }
    }
}
