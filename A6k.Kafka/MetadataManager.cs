using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using A6k.Kafka.Messages;

namespace A6k.Kafka
{
    public class MetadataManager : IAsyncDisposable
    {
        private readonly KafkaConnectionFactory kafkaConnectionFactory;
        private readonly ILogger<MetadataManager> logger;

        private ConcurrentDictionary<int, Broker> brokers = new ConcurrentDictionary<int, Broker>();
        private TopicMetadataCache topics;

        public MetadataManager(KafkaConnectionFactory kafkaConnectionFactory, ILogger<MetadataManager> logger)
        {
            this.kafkaConnectionFactory = kafkaConnectionFactory ?? throw new ArgumentNullException(nameof(kafkaConnectionFactory));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public string ClusterId { get; private set; }
        public int ControllerId { get; private set; }

        public IEnumerable<Broker> Brokers => brokers.Values;

        public async Task Connect(string clientId, string bootstrapServers)
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
                    {
                        logger.LogInformation("Discovered: {broker}", server);
                        brokers[broker.NodeId] = new Broker(broker, kafkaConnectionFactory);
                    }

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
                await broker.Connect(clientId);

            topics = new TopicMetadataCache(this);
        }

        public async Task Disconnect()
        {
            while (brokers.Count > 0)
            {
                foreach (var b in brokers.Values)
                {
                    await b.DisposeAsync();
                    brokers.TryRemove(b.NodeId, out var _);
                }
            }
        }

        public Broker GetBroker(int nodeId)
        {
            if (brokers.TryGetValue(nodeId, out var broker))
                return broker;
            throw new InvalidOperationException($"Broker ({nodeId}) not known");
        }

        public Broker GetRandomBroker()
        {
            // not random, but this'll do for now
            return brokers.Values.First();
        }

        public ValueTask<MetadataResponse.TopicMetadata> GetTopic(string topicName) => topics.GetTopic(topicName);

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
            public IReadOnlyList<ApiVersion> ApiVersions { get; private set; }

            public async Task Connect(string clientId)
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

        private class TopicMetadataCache
        {
            private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(300);

            private readonly MetadataManager metadataManager;
            private readonly TimeSpan timeout;
            private readonly MemoryCache memoryCache = new MemoryCache(Options.Create(new MemoryCacheOptions()));

            public TopicMetadataCache(MetadataManager metadataManager) : this(metadataManager, DefaultTimeout) { }

            public TopicMetadataCache(MetadataManager metadataManager, TimeSpan timeout)
            {
                this.metadataManager = metadataManager;
                this.timeout = timeout;
            }

            public async ValueTask<MetadataResponse.TopicMetadata> GetTopic(string topicName)
            {
                // TODO: Lock?

                // find or get the metadata for the topicName
                return await memoryCache.GetOrCreateAsync(topicName, async entry =>
                {
                    var broker = metadataManager.GetRandomBroker();
                    var md = await broker.Connection.Metadata(topicName);
                    entry.AbsoluteExpirationRelativeToNow = timeout;
                    return md.Topics[0];
                });
            }

            public async Task RefreshAllTopics()
            {
                var broker = metadataManager.GetRandomBroker();
                var md = await broker.Connection.Metadata(string.Empty);
                foreach (var topicMetadata in md.Topics)
                {
                    memoryCache.Set(topicMetadata.TopicName, topicMetadata, timeout);
                }
            }
        }
    }
}
