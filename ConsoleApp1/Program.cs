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
        static void Main(string[] args)
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

            Console.WriteLine("Produce -----");

            //Produce();

            Console.WriteLine("done...");
            Console.ReadLine();
        }

        private static void Produce()
        {
            var producer = new ProducerBuilder<string, string>(
                new AdminClientConfig()
                {
                    BootstrapServers = "localhost:29092"
                })
                .Build();

            producer.Produce("test-topic", new Message<string, string> { Key = "fred", Value = "flintstone" });
            producer.Produce("test-topic", new Message<string, string> { Key = "wilma", Value = "flintstone" });

            var h = new Headers();
            h.Add("age", new byte[] { 0 });
            producer.Produce("test-topic", new Message<string, string> { Key = "bambam", Value = "flintstone", Headers = h });
        }

        private static async Task Consume(CancellationToken cancellationToken)
        {
            await Task.Yield();
            Console.WriteLine("consuming...");
            var consumer = new ConsumerBuilder<string, string>(
                    new ConsumerConfig
                    {
                        BootstrapServers = "localhost:29092",
                        GroupId = "testgroup-" + Guid.NewGuid().ToString(),
                        EnablePartitionEof = true
                    }
                )
                .SetPartitionsAssignedHandler((_, p) =>p.Select(tp => new TopicPartitionOffset(tp, Offset.Beginning)).ToList())
                .Build();

            consumer.Subscribe("test-topic");

            var timeout = TimeSpan.FromMilliseconds(200);
            while (!cancellationToken.IsCancellationRequested)
            {
                var msg = consumer.Consume(timeout);
                if (msg == null)
                    continue;
                if (msg.IsPartitionEOF)
                    break;

                Console.WriteLine($"key={msg.Key} value={msg.Value}");
            }

            consumer.Dispose();
            Console.WriteLine("...consumer stopped");
        }

        private static void DoAdmin()
        {
            Console.WriteLine("Admin -----");

            var admin = new AdminClientBuilder(
                new AdminClientConfig()
                {
                    BootstrapServers = "localhost:29092",
                    ApiVersionRequest = true
                })
                .Build();

            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
            Console.WriteLine($"{metadata.OriginatingBrokerId} - {metadata.OriginatingBrokerName}");
            foreach (var topic in metadata.Topics)
                Console.WriteLine($"  {topic.Topic}");
        }
    }
}
