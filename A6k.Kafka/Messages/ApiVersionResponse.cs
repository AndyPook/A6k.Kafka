using Bedrock.Framework.Protocols;

namespace A6k.Kafka.Messages
{
    public class ApiVersionResponse
    {
        public short ErrorCode { get; set; }
        public ApiVersion[] ApiVersions { get; set; }
        public class ApiVersion
        {
            public short ApiKey { get; set; }
            public short MinVersion { get; set; }
            public short MaxVersion { get; set; }
        }
    }
}
