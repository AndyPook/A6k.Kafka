using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using A6k.Kafka;
using Bedrock.Framework;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace TestConsole
{
    // simple console app to do *basic* tests/confirmations against a single broker Kafka instance 

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

            Console.WriteLine(BitConverter.IsLittleEndian ? "little" : "big");

            await using var connection = await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 29092));
            var kafka = new KafkaProtocol(connection, "fred");
            Console.WriteLine("\n\n");

            //await GetVersion(kafka);
            //await GetMetadata(kafka);
            //await Produce(kafka);
            //await FindCoordinator(kafka);
            await Fetch(kafka);

            Console.WriteLine("done...");
            Console.ReadLine();
        }

        private static async Task Fetch(KafkaProtocol kafka)
        {
            Console.WriteLine("----- Fetch");

            var req = new FetchRequest
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
                        TopicName="test-topic",
                        Partitions= new FetchRequest.Topic.Partition[]
                        {
                            new FetchRequest.Topic.Partition
                            {
                                PartitionId=0,
                                CurrentLeaderEpoc=-1,
                                FetchOffset=0,
                                LogStartOffset=-1,
                                PartitionMaxBytes=32*1024
                            }
                        }
                    }
                }
            };

            var response = await kafka.Fetch(req);
            Console.WriteLine($"throttle:     {response.ThrottleTime}");
            Console.WriteLine($"errorCode:    {response.ErrorCode}");

            foreach (var r in response.Responses)
            {
                Console.WriteLine($"topic: {r.TopicName}");
                foreach (var pr in r.PartitionResponses)
                {
                    Console.WriteLine("  partition: " + pr.PartitionId);
                    foreach (var batch in pr.RecordBatches)
                    {
                        Console.WriteLine("    batch offset: " + batch.BaseOffset);
                        foreach (var rec in batch.Records)
                        {
                            Console.WriteLine("      offset: " + rec.Offset);
                            Console.WriteLine("      key   : " + Encoding.UTF8.GetString(rec.Key));
                            Console.WriteLine("      val   : " + Encoding.UTF8.GetString(rec.Value));
                        }
                    }
                }
            }
        }

        private static async Task FindCoordinator(KafkaProtocol kafka)
        {
            Console.WriteLine("----- FindCoordinator");

            var response = await kafka.FindCoordinator(Guid.NewGuid().ToString());
            Console.WriteLine($"throttle:     {response.ThrottleTime}");
            Console.WriteLine($"errorCode:    {response.ErrorCode}");
            Console.WriteLine($"errorMessage: {response.ErrorMessage}");
            Console.WriteLine($"nodeId:       {response.NodeId}");
            Console.WriteLine($"host:         {response.Host}");
            Console.WriteLine($"port:         {response.Port}");
        }
        private static async Task JoinGroup(KafkaProtocol kafka)
        {
            Console.WriteLine("----- JoinGroup");

            var response = await kafka.JoinGroup(new JoinGroupRequest
            {
                GroupId = "test_" + Guid.NewGuid().ToString(),
                SessionTimeout = 10000,
                RebalanceTimeout = 300000,
                ProtocolType = "consumer",
                Protocols = new JoinGroupRequest.Protocol[]
                {
                    new JoinGroupRequest.Protocol{ Name= "range", }
                }
            });

            Console.WriteLine($"throttle:     {response.ThrottleTime}");
            Console.WriteLine($"errorCode:    {response.ErrorCode}");
            Console.WriteLine($"genId:        {response.GenerationId}");
            Console.WriteLine($"ProtocolName: {response.ProtocolName}");
            Console.WriteLine($"Leader:       {response.Leader}");
            Console.WriteLine($"memberId:     {response.MemberId}");
            foreach (var m in response.Members)
                Console.WriteLine($"  memberId:{m.MemberId} group:{m.GroupInstanceId} meta:{(m.Metadata?.Length.ToString() ?? "NONE")}");
        }

        private static async Task Produce(KafkaProtocol kafka)
        {
            Console.WriteLine("----- Produce");

            var msg = new Message<string, string>
            {
                Key = "fred",
                Value = "flintstone " + Guid.NewGuid().ToString()
            };
            Console.WriteLine(msg.Value);

            var response = await kafka.Produce("test-topic", msg, IntrinsicWriter.String, IntrinsicWriter.String);
            Console.WriteLine($"throttle: {response.ThrottleTime}");
            foreach (var r in response.Responses)
            {
                Console.WriteLine($"topic: {r.Topic}");
                foreach (var p in r.Partitions)
                    Console.WriteLine($"    {p.Partition} (error@ {p.ErrorCode}) - {p.BaseOffset} {p.LogStartOffset} {Timestamp.UnixTimestampMsToDateTime(p.LogAppendTime)}");
            }
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
            Console.WriteLine("----- Metadata");
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
