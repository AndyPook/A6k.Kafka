using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using A6k.Kafka.Metadata;

namespace A6k.Kafka
{
    public class ClusterManager : IAsyncDisposable
    {
        private readonly KafkaConnectionFactory kafkaConnectionFactory;
        private readonly ILogger<ClusterManager> logger;

        private ConcurrentDictionary<int, BrokerConnection> brokers = new ConcurrentDictionary<int, BrokerConnection>();
        private TopicMetadataCache topics;

        public ClusterManager(string clientId, KafkaConnectionFactory kafkaConnectionFactory, ILogger<ClusterManager> logger)
        {
            ClientId = clientId;
            this.kafkaConnectionFactory = kafkaConnectionFactory ?? throw new ArgumentNullException(nameof(kafkaConnectionFactory));
            this.logger = logger ?? throw new ArgumentNullException(nameof(logger));
            this.topics = new TopicMetadataCache(this);
        }

        public string ClientId { get; }
        public string ClusterId { get; private set; }
        public int ControllerId { get; private set; }

        public IEnumerable<BrokerConnection> Brokers => brokers.Values;

        public async Task Connect(string bootstrapServers)
        {
            var servers = bootstrapServers.Split(';');

            foreach (var server in servers)
            {
                try
                {
                    await using var kafka = kafkaConnectionFactory.CreateConnection(server, ClientId);
                    var meta = await kafka.Metadata(null);

                    ClusterId = meta.ClusterId;
                    ControllerId = meta.ControllerId;
                    foreach (var b in meta.Brokers)
                    {
                        logger.LogInformation("Discovered: {broker}", server);
                        var broker = new Broker(
                            b.NodeId,
                            b.Host,
                            b.Port,
                            b.Rack
                        );
                        brokers[broker.NodeId] = new BrokerConnection(broker, kafkaConnectionFactory.CreateClient(), ClientId);
                    }

                    break;
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Broker connection problem: {server}", server);
                }
            }
            if (brokers.Count == 0)
                throw new InvalidOperationException("no brokers reachable via: " + bootstrapServers);

            //foreach (var broker in brokers.Values)
            //    await broker.Open(ClientId);
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

        public BrokerConnection GetBroker(int nodeId)
        {
            if (brokers.TryGetValue(nodeId, out var broker))
                return broker;
            throw new InvalidOperationException($"Broker ({nodeId}) not known");
        }

        public BrokerConnection GetRandomBroker()
        {
            // not random, but this'll do for now
            return brokers.Values.First();
        }

        public ValueTask<TopicMetadata> GetTopic(string topicName) => topics.GetTopic(topicName);

        public async ValueTask DisposeAsync()
        {
            foreach (var b in Brokers)
                await b.DisposeAsync();
        }

        private class TopicMetadataCache
        {
            private static readonly TimeSpan DefaultTimeout = TimeSpan.FromSeconds(300);

            private readonly ClusterManager metadataManager;
            private readonly TimeSpan timeout;
            private readonly MemoryCache memoryCache = new MemoryCache(Options.Create(new MemoryCacheOptions()));

            public TopicMetadataCache(ClusterManager metadataManager) : this(metadataManager, DefaultTimeout) { }

            public TopicMetadataCache(ClusterManager metadataManager, TimeSpan timeout)
            {
                this.metadataManager = metadataManager;
                this.timeout = timeout;
            }

            public async ValueTask<TopicMetadata> GetTopic(string topicName)
            {
                // TODO: Lock?

                // find or get the metadata for the topicName
                return await memoryCache.GetOrCreateAsync(topicName, async entry =>
                {
                    entry.AbsoluteExpirationRelativeToNow = timeout;
                    return await GetTopicMetaData(topicName);
                });
            }

            public async ValueTask<TopicMetadata> RefreshTopic(string topicName)
            {
                var topic = await GetTopicMetaData(topicName);
                return memoryCache.Set(topicName, topic, timeout);
            }

            private async ValueTask<TopicMetadata> GetTopicMetaData(string topicName)
            {
                var broker = metadataManager.GetRandomBroker();
                var topicResponse = await broker.Metadata(topicName);
                var t = topicResponse.Topics[0];
                return new TopicMetadata(
                    t.TopicName,
                    t.IsInternal,
                    t.Partitions
                        .Select(p => new PartitionMetadata(p.PartitionId, p.Leader, p.Replicas.ToArray(), p.Isr.ToArray()))
                        .ToArray()
                );
            }

            public async Task RefreshAllTopics()
            {
                var broker = metadataManager.GetRandomBroker();
                var md = await broker.Metadata(string.Empty);
                foreach (var topicMetadata in md.Topics)
                    memoryCache.Set(topicMetadata.TopicName, topicMetadata, timeout);
            }
        }
    }
}
