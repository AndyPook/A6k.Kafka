using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

using A6k.Kafka.Messages;
using A6k.Kafka.Metadata;

namespace A6k.Kafka
{
    public class Consumer<TKey, TValue>
    {
        private readonly MetadataManager metadataManager;
        private readonly string clientId;
        private readonly string bootstrapServers;

        private IDeserializer<TKey> keyDeserializer;
        private IDeserializer<TValue> valueDeserializer;

        private TopicMetadata topicMetadata;

        private ChannelReader<Message<TKey, TValue>> messageReader;

        public Consumer(MetadataManager metadataManager, string clientId, string bootstrapServers)
        {
            this.metadataManager = metadataManager ?? throw new ArgumentNullException(nameof(metadataManager));
            this.clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
            this.bootstrapServers = bootstrapServers ?? throw new ArgumentNullException(nameof(bootstrapServers));

            if (!IntrinsicReader.TryGetDeserializer(out keyDeserializer))
                throw new ArgumentException($"No deserializer found for Key ({typeof(TKey).Name})");
            if (!IntrinsicReader.TryGetDeserializer(out valueDeserializer))
                throw new ArgumentException($"No deserializer found for Value ({typeof(TValue).Name})");
        }

        public async Task Subscribe(string topicName)
        {
            await metadataManager.Connect(clientId, bootstrapServers);
            topicMetadata = await metadataManager.GetTopic(topicName);
            Fetch();
        }

        public ValueTask<Message<TKey, TValue>> Consume()
        {
            if (messageReader.TryRead(out var msg))
                return new ValueTask<Message<TKey, TValue>>(msg);

            // effectively end-of-partition
            return new ValueTask<Message<TKey, TValue>>();
        }

        private void Fetch(CancellationToken cancellationToken = default)
        {
            var channel = Channel.CreateBounded<Message<TKey, TValue>>(new BoundedChannelOptions(100) { SingleReader = true });
            messageReader = channel.Reader;
            var messageWriter = channel.Writer;

            foreach (var p in topicMetadata.Partitions)
            {
                var broker = metadataManager.GetBroker(p.Leader);
                _ = Fetch(messageWriter, topicMetadata.TopicName, p, broker, cancellationToken);
            }
        }

        private async Task Fetch(ChannelWriter<Message<TKey, TValue>> messageWriter, string topic, PartitionMetadata partition, Broker broker, CancellationToken cancellationToken)
        {
            long offset = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
                var fetch = await broker.Connection.Fetch(new FetchRequest
                {
                    ReplicaId = -1,
                    MaxWaitTime = 100,
                    MinBytes = 1,
                    MaxBytes = 64 * 1024,
                    IsolationLevel = 0,
                    SessionId = 0,
                    SessionEpoc = -1,
                    Topics = new []
                    {
                            new FetchRequest.Topic
                            {
                                TopicName = topic,
                                Partitions = new []
                                {
                                    new FetchRequest.Topic.Partition
                                    {
                                        PartitionId = partition.PartitionId,
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
                                    Key = await keyDeserializer.Deserialize(rec.Key),
                                    Value = await valueDeserializer.Deserialize(rec.Value)
                                };

                                if (rec.Headers.Count > 0)
                                {
                                    foreach (var h in rec.Headers)
                                        msg.AddHeader(h.Key, h.Value);
                                }

                                await messageWriter.WriteAsync(msg, cancellationToken);
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

    public class ConsumerPartitionFetcher
    {
        private async Task Fetch(ChannelWriter<RecordBatch.Record> messageWriter, string topic, PartitionMetadata partition, Broker broker, CancellationToken cancellationToken)
        {
            long offset = 0;
            while (!cancellationToken.IsCancellationRequested)
            {
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
                                TopicName = topic,
                                Partitions = new FetchRequest.Topic.Partition[]
                                {
                                    new FetchRequest.Topic.Partition
                                    {
                                        PartitionId = partition.PartitionId,
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

                                await messageWriter.WriteAsync(rec, cancellationToken);
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
