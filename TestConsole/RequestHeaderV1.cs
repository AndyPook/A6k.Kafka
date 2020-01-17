namespace TestConsole
{
    public struct RequestHeaderV1
    {
        public short ApiKey { get; set; }
        public short ApiVersion { get; set; }
        public int CorrelationId { get; set; }
        public string ClientId { get; set; }
        public object Request { get; set; }
    }

}
