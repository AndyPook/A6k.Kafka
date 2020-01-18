using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using A6k.Kafka;
using Bedrock.Framework;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TestConsole
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var serviceProvider =
                new ServiceCollection()
                .AddLogging(builder =>
                {
                    builder.SetMinimumLevel(LogLevel.Warning);
                    builder.AddConsole();
                })
              .BuildServiceProvider();

            var client = new ClientBuilder(serviceProvider)
                .UseSockets()
                .UseConnectionLogging()
                .Build();

            await using var connection = await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 29092));
            var kafka = new KafkaProtocol(connection, "fred");

            await GetVersion(kafka);

            await GetMetadata(kafka);

            Console.WriteLine("done...");
            Console.ReadLine();
        }

        private static async Task GetVersion(KafkaProtocol kafka)
        {
            Console.WriteLine("----- ApiVersion");
            var apiversions = await kafka.ApiVersion();
            Console.WriteLine("error: " + apiversions.ErrorCode);
            foreach (var v in apiversions.ApiVersions)
                Console.WriteLine($"{ApiKey.GetName(v.ApiKey).PadRight(20)} {v.MinVersion}->{v.MaxVersion}");
        }

        private static async Task GetMetadata(KafkaProtocol kafka)
        {
            Console.WriteLine("\n\n----- Metadata");
            var metadata = await kafka.Metadata();
            Console.WriteLine("brokers");
            foreach (var b in metadata.Brokers)
                Console.WriteLine($"  {b.NodeId}: {b.Host}:{b.Port}");
            Console.WriteLine($"ClusterId:{metadata.ClusterId} ControllerId:{metadata.ControllerId}");
            Console.WriteLine("topics");
            foreach (var t in metadata.Topics)
            {
                Console.WriteLine($"  {t.TopicName} (Internal={t.IsInternal})");
                //Console.WriteLine($"    " + string.Join(", ", t.Partitions.Select(p => $"{ p.PartitionId}({p.Leader}")));
            }
        }
    }
}
