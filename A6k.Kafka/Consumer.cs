using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;
using A6k.Kafka.Messages;

namespace A6k.Kafka
{
    public class Consumer<TKey, TValue>
    {
        private readonly MetadataManager metadataManager;
        private readonly string clientId;
        private readonly string bootstrapServers;

        private IDeserializer<TKey> keyDeserializer;
        private IDeserializer<TValue> valueDeserializer;

        private MetadataResponse.TopicMetadata topicMetadata;

        private ConcurrentQueue<Message<TKey, TValue>> messageQueue = new ConcurrentQueue<Message<TKey, TValue>>();

        public Consumer(MetadataManager metadataManager, string clientId, string bootstrapServers)
        {
            this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));
            this.clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
            this.bootstrapServers = bootstrapServers ?? throw new ArgumentNullException(nameof(bootstrapServers));

            if (!IntrinsicReader.TryGetDeserializer<TKey>(out keyDeserializer))
                throw new ArgumentException($"No deserializer found for Key ({typeof(TKey).Name})");
            if (!IntrinsicReader.TryGetDeserializer<TValue>(out valueDeserializer))
                throw new ArgumentException($"No deserializer found for Value ({typeof(TValue).Name})");
        }


        public async Task Subscribe(string topicName)
        {
            await metadataManager.Init(clientId, bootstrapServers);
            topicMetadata = await metadataManager.Topics.GetTopic(topicName);
            _ = Fetch();
        }

        public ValueTask<Message<TKey, TValue>> Consume()
        {
            if (messageQueue.TryDequeue(out var msg))
                return new ValueTask<Message<TKey, TValue>>(msg);

            return new ValueTask<Message<TKey, TValue>>();
        }

        private async Task Fetch()
        {
            long offset = 0;
            while (true)
            {
                foreach (var p in topicMetadata.Partitions)
                {
                    var broker = metadataManager.GetBroker(p.Leader);
                    var fetch = await broker.Connection.Fetch(new FetchRequest
                    {
                        ReplicaId = -1,
                        MaxWaitTime = 100,
                        MinBytes = 1,
                        MaxBytes = 64 * 1024,
                        IsolationLevel = 0,
                        SessionId = 0,
                        SessionEpoc = -1,
                        Topics = new FetchRequest.Topic[]
                        {
                            new FetchRequest.Topic
                            {
                                TopicName = topicMetadata.TopicName,
                                Partitions = new FetchRequest.Topic.Partition[]
                                {
                                    new FetchRequest.Topic.Partition
                                    {
                                        PartitionId = p.PartitionId,
                                        CurrentLeaderEpoc = -1,
                                        FetchOffset = offset,
                                        LogStartOffset = -1,
                                        PartitionMaxBytes = 32*1024
                                    }
                                }
                            }
                        }
                    });

                    long maxoffset = 0;
                    long high = 0;
                    foreach (var r in fetch.Responses)
                    {
                        foreach (var pr in r.PartitionResponses)
                        {
                            high = pr.HighWaterMark;
                            foreach (var batch in pr.RecordBatches)
                            {
                                foreach (var rec in batch.Records)
                                {
                                    if (rec.Offset > maxoffset)
                                        maxoffset = rec.Offset;

                                    var msg = new Message<TKey, TValue>
                                    {
                                        Timestamp = Timestamp.UnixTimestampMsToDateTime(batch.FirstTimeStamp + rec.TimeStampDelta),
                                        Topic = r.TopicName,
                                        PartitionId = pr.PartitionId,
                                        Offset = rec.Offset,
                                        Key = keyDeserializer.Deserialize(rec.Key),
                                        Value = valueDeserializer.Deserialize(rec.Value)
                                    };

                                    if (rec.Headers.Count > 0)
                                    {
                                        foreach (var h in rec.Headers)
                                            msg.AddHeader(h.Key, h.Value);
                                    }

                                    messageQueue.Enqueue(msg);
                                }
                            }
                        }
                    }

                    if (maxoffset > 0)
                        offset = maxoffset + 1;
                    if (offset >= high)
                        await Task.Delay(200); // linger
                }
            }
        }
    }
}
