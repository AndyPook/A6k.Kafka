using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace A6k.Kafka.Metadata
{
    public class Broker : IAsyncDisposable, IEquatable<Broker>
    {
        private readonly KafkaConnectionFactory kafkaConnectionFactory;

        public Broker(int nodeId, string host, int port, string rack, KafkaConnectionFactory kafkaConnectionFactory)
        {
            // TODO: guard statements

            NodeId = nodeId;
            Host = host;
            Port = port;
            Rack = rack;
            this.kafkaConnectionFactory = kafkaConnectionFactory ?? throw new ArgumentNullException(nameof(kafkaConnectionFactory));
        }

        public int NodeId { get; }
        public string Host { get; }
        public int Port { get; }
        public string Rack { get; }

        public KafkaConnection Connection { get; private set; }
        public IReadOnlyList<ApiVersion> ApiVersions { get; private set; }

        public async Task Start(string clientId)
        {
            Connection = await kafkaConnectionFactory.CreateConnection(Host, Port, clientId);
            var version = await Connection.ApiVersion();
            ApiVersions = version.ApiVersions.Select(x => new ApiVersion(x.ApiKey, x.MinVersion, x.MaxVersion)).ToArray();
        }

        public bool Equals(string host, int port, string rack = null)
        {
            if (rack == null)
                return string.Equals(Host, host) && Port == port;

            return string.Equals(Host, host) && Port == port && string.Equals(Rack, rack);
        }

        public override bool Equals(object obj) => obj is Broker b && Equals(b);

        public bool Equals([AllowNull] Broker other)
        {
            if (ReferenceEquals(null, other))
                return false;
            if (ReferenceEquals(this, other))
                return true;

            return
                NodeId == other.NodeId &&
                string.Equals(Host, other.Host) &&
                Port == other.Port &&
                string.Equals(Rack, other.Rack);
        }

        public override int GetHashCode() => HashCode.Combine(NodeId, Host, Port, Rack);

        public override string ToString() => $"node: {NodeId}: {Host}:{Port} ({Rack})";

        public ValueTask DisposeAsync() => Connection.DisposeAsync();
    }
}
