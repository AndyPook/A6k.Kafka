namespace A6k.Kafka.Metadata
{
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
