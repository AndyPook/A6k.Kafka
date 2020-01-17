using System;
using Confluent.Kafka;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Hello World!");

            var admin = new AdminClientBuilder(
                new AdminClientConfig()
                {
                    BootstrapServers = "localhost:29092",
                    ApiVersionRequest = true
                })
                .Build();

            var metadata = admin.GetMetadata(TimeSpan.FromSeconds(5));
            Console.WriteLine($"{metadata.OriginatingBrokerId} - {metadata.OriginatingBrokerName}");
            foreach(var topic in metadata.Topics)
                Console.WriteLine($"  {topic.Topic}");

            Console.WriteLine("done...");
            Console.ReadLine();
        }
    }
}
