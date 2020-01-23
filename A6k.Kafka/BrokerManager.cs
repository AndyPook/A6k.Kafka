using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

using A6k.Kafka.Messages;
using System.Linq;

namespace A6k.Kafka
{
    public class BrokerManager : IAsyncDisposable
    {
        private readonly KafkaConnectionFactory kafkaConnectionFactory;
        private readonly ILogger<BrokerManager> logger;
        private Dictionary<int, Broker> brokers = new Dictionary<int, Broker>();

        public BrokerManager(KafkaConnectionFactory kafkaConnectionFactory, ILogger<BrokerManager> logger)
        {
            this.kafkaConnectionFactory = kafkaConnectionFactory ?? throw new ArgumentNullException(nameof(kafkaConnectionFactory));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public string ClusterId { get; private set; }
        public int ControllerId { get; private set; }

        public Broker GetBroker(int nodeId)
        {
            if (brokers.TryGetValue(nodeId, out var broker))
                return broker;
            throw new InvalidOperationException($"Broker ({nodeId}) not found");
        }

        public IEnumerable<Broker> Brokers => brokers.Values;

        public async Task Init(string clientId, string bootstrapServers)
        {
            var servers = bootstrapServers.Split(';');

            foreach (var server in servers)
            {
                try
                {
                    await using var kafka = await kafkaConnectionFactory.CreateConnection(server, clientId);
                    var meta = await kafka.Metadata(null);

                    ClusterId = meta.ClusterId;
                    ControllerId = meta.ControllerId;
                    foreach (var broker in meta.Brokers)
                        brokers[broker.NodeId] = new Broker(broker, kafkaConnectionFactory);

                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Broker connection problem: {server}", server);
                }
            }
            if (brokers.Count == 0)
                throw new InvalidOperationException("no reachable brokers via: " + bootstrapServers);

            foreach (var broker in brokers.Values)
                await broker.Init(clientId);
        }

        public async ValueTask DisposeAsync()
        {
            foreach (var b in Brokers)
                await b.DisposeAsync();
        }

        public class Broker : IAsyncDisposable
        {
            private readonly KafkaConnectionFactory kafkaConnectionFactory;

            public Broker(MetadataResponse.Broker broker, KafkaConnectionFactory kafkaConnectionFactory)
            {
                _ = broker ?? throw new ArgumentNullException(nameof(broker));
                NodeId = broker.NodeId;
                Host = broker.Host;
                Port = broker.Port;
                Rack = broker.Rack;
                this.kafkaConnectionFactory = kafkaConnectionFactory ?? throw new ArgumentNullException(nameof(kafkaConnectionFactory));
            }

            public int NodeId { get; }
            public string Host { get; }
            public int Port { get; }
            public string Rack { get; }

            public KafkaConnection Connection { get; private set; }
            public IReadOnlyCollection<ApiVersion> ApiVersions { get; set; }

            public async Task Init(string clientId)
            {
                Connection = await kafkaConnectionFactory.CreateConnection(Host, Port, clientId);
                var version = await Connection.ApiVersion();
                ApiVersions = version.ApiVersions.Select(x => new ApiVersion(x.ApiKey, x.MinVersion, x.MaxVersion)).ToArray();
            }

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

            public ValueTask DisposeAsync() => Connection.DisposeAsync();
        }
    }
}
