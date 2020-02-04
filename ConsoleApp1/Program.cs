using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace ConsoleApp1
{
    // uses Confluent.Kafka to generate some traffix to analyse with WireShark

    class Program
    {
        static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                Console.WriteLine("cancelled...");
                cts.Cancel();
                e.Cancel = true;
            };

            //DoAdmin();

            _ = Consume(cts.Token);

            //Produce();

            await GetGroup();

            Console.WriteLine("done...");
            Console.ReadLine();
        }

        private static void Produce()
        {
            Console.WriteLine("Produce -----");

            var producer = new ProducerBuilder<string, string>(
                new AdminClientConfig()
                {
                    BootstrapServers = "localhost:29092"
                })
                .Build();

            producer.Produce("partitioned-topic", new Message<string, string> { Key = "fred", Value = "flintstone" });
            producer.Produce("partitioned-topic", new Message<string, string> { Key = "wilma", Value = "flintstone" });

            var h = new Headers();
            h.Add("age", new byte[] { 0 });
            producer.Produce("partitioned-topic", new Message<string, string> { Key = "bambam", Value = "flintstone", Headers = h });
        }

        private static async Task Consume(CancellationToken cancellationToken)
        {
            await Task.Yield();
            Console.WriteLine("consuming...");
            var consumer = new ConsumerBuilder<string, string>(
                    new ConsumerConfig
                    {
                        BootstrapServers = "localhost:29092",
                        GroupId = "testgroup",
                        EnablePartitionEof = true,
                        FetchWaitMaxMs = 10_000 // make Fetch less agressive
                    }
                )
                .SetPartitionsAssignedHandler((_, p) => p.Select(tp => new TopicPartitionOffset(tp, Offset.Beginning)).ToList())
                .Build();

            consumer.Subscribe("partitioned-topic");

            var timeout = TimeSpan.FromMilliseconds(100);
            while (!cancellationToken.IsCancellationRequested)
            {
                var msg = consumer.Consume(timeout);
                if (msg == null || msg.IsPartitionEOF)
                    continue;

                Console.WriteLine($"key={msg.Key} value={msg.Value} (p:{msg.Partition.Value} o:{msg.Offset.Value})");
            }

            consumer.Dispose();
            Console.WriteLine("...consumer stopped");
        }

        private static void DoAdmin(bool getTopics = true, bool getGroups = true)
        {
            var admin = new AdminClientBuilder(
                new AdminClientConfig()
                {
                    BootstrapServers = "localhost:29092",
                    ApiVersionRequest = true
                })
                .Build();

            if (getTopics)
            {
                Console.WriteLine("\n\n---- topics");
                var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
                Console.WriteLine($"{metadata.OriginatingBrokerId} - {metadata.OriginatingBrokerName}");
                foreach (var topic in metadata.Topics)
                    Console.WriteLine($"  {topic.Topic}");
            }

            if (getGroups)
            {
                while (true)
                {
                    // FYI: ListGroups is broken :( (see https://github.com/confluentinc/confluent-kafka-dotnet/pull/1169)
                    var g = admin.ListGroup("testgroup", TimeSpan.FromSeconds(1));
                    if (g != null && g.Members?.Count > 0)
                    {
                        Console.WriteLine($"group: {g.Group} - {string.Join(",", g.Members?.Select(m => m.MemberId))}");
                        break;
                    }
                }
            }
        }
        private static async Task GetGroup()
        {
            var admin = new AdminClientBuilder(
                new AdminClientConfig()
                {
                    BootstrapServers = "localhost:29092",
                    ApiVersionRequest = true
                })
                .Build();

            while (true)
            {
                // FYI: ListGroups is broken :( (see https://github.com/confluentinc/confluent-kafka-dotnet/pull/1169)
                var g = admin.ListGroup("testgroup", TimeSpan.FromSeconds(1));
                if (g != null && g.Members?.Count > 0)
                {
                    Console.WriteLine($"group: {g.Group} - {string.Join(",", g.Members?.Select(m => m.MemberId))}");
                    break;
                }

                await Task.Delay(2_000);
            }
        }
    }
}
