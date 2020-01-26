
using System.Collections.Generic;

namespace A6k.Kafka.Messages
{
    public class ApiVersionResponse
    {
        public ApiVersionResponse(short errorCode)
        {
            ErrorCode = errorCode;
            ApiVersions = new ApiVersion[0];
        }

        public ApiVersionResponse(ApiVersion[] apiVersions)
        {
            ApiVersions = apiVersions;
        }

        public short ErrorCode { get; }
        public IReadOnlyList<ApiVersion> ApiVersions { get; }

        public class ApiVersion
        {
            public ApiVersion(short apiKey, short minVersion, short maxVersion)
            {
                ApiKey = apiKey;
                MinVersion = minVersion;
                MaxVersion = maxVersion;
            }

            public short ApiKey { get; }
            public short MinVersion { get; }
            public short MaxVersion { get; }
        }
    }
}
