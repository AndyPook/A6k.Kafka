﻿using System;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using A6k.Kafka;
using A6k.Kafka.Messages;
using Bedrock.Framework;

namespace TestConsole
{
    // simple console app to do *basic* tests/confirmations against a single broker Kafka instance 

    class Program
    {
        private const string BootstrapServers = "localhost:29092";
        //private const string TopicName = "test-topic";
        private const string TopicName = "partitioned-topic";

        static async Task Main(string[] args)
        {
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                Console.WriteLine("cancelled...");
                cts.Cancel();
                e.Cancel = true;
            };

            //var kafka = GetKafka();
            //await GetVersion(kafka);
            //await GetMetadata(kafka);
            //await Produce(kafka);
            //await FindCoordinator(kafka);
            //while (!cts.IsCancellationRequested)
            //{
            //    await kafka.Heartbeat(new HeartbeatRequest
            //    {
            //        GroupId = "groupId",
            //        GenerationId = 0,
            //        MemberId = "12345"
            //    });
            //    await Task.Delay(500);
            //}

            //var kafka = await GetKafka(29093);
            //await Fetch(kafka);
            //await Fetch(kafka, offset: 1);
            //await Fetch(kafka, offset: 2);
            //await Fetch(kafka, offset: 3);
            //await Fetch(kafka, offset: 4);
            //await Fetch(kafka, offset: 5);
            //await Fetch(kafka);

            //await BrokerMgr();

            //await Producer();

            //await Consumer();

            _ = ConsumerGroup();

            Console.WriteLine("ctrl-C to end");
            // wait forever and do not throw on cancellation
            // doing it this way means ConsumerGroup connections do not block as they do with Console.Readline()
            await Task.Delay(-1, cts.Token).ContinueWith(_ => { }, TaskContinuationOptions.OnlyOnCanceled);
        }

        private static async Task BrokerMgr()
        {
            var cluster = GetClusterManager();
            await cluster.Connect(BootstrapServers);

            foreach (var b in cluster.Brokers)
            {
                Console.WriteLine($"broker: {b.NodeId} - {b.Host}:{b.Port} (rack={b.Rack})");
                Console.WriteLine("  " + string.Join(",", b.ApiVersions.Select(x => $"{x.ApiKey}({x.MinVersion}-{x.MaxVersion})")));
            }
        }

        private static async Task Producer()
        {
            var cluster = GetClusterManager();
            await cluster.Connect("fred-producer");
            var producer = new Producer<string, string>(TopicName, cluster);

            var response = await producer.Produce("fred", Guid.NewGuid().ToString());

            Console.WriteLine($"throttle: {response.ThrottleTime}");
            foreach (var r in response.Topics)
            {
                Console.WriteLine($"topic: {r.Topic}");
                foreach (var p in r.Partitions)
                    Console.WriteLine($"    {p.Partition} (error: {p.ErrorCode.ToString()}) - {p.BaseOffset} {p.LogStartOffset} {Timestamp.UnixTimestampMsToDateTime(p.LogAppendTime)}");
            }
        }


        private static async Task ConsumerGroup()
        {
            var cluster = GetClusterManager();
            await cluster.Connect(BootstrapServers);

            var coordinator = new ClientGroupCoordinator(cluster, "testgroup", TopicName);
            coordinator.StateChanged += (c, oldState, newState) => Console.WriteLine($"state: {oldState} -> {newState}");
            coordinator.CoordinatorFound += c => Console.WriteLine($"coordinator: {c.CoordinatorId}");
            coordinator.GroupJoined += c => Console.WriteLine($"joined: memberId: {c.MemberId} ({(c.IsLeader ? "leader" : "member")}) - Protocol: {c.GroupProtocol}");
            coordinator.GroupSynced += c =>
            {
                Console.WriteLine("synced: group version: " + c.CurrentMemberState.Version);
                foreach (var a in c.CurrentMemberState.AssignedPartitions)
                    Console.WriteLine($"  assigned: {a.Topic}: {string.Join(",", a.Partitions)}");
            };

            await coordinator.Start();
        }

        private static async Task Consumer()
        {
            var cluster = GetClusterManager();
            await cluster.Connect(BootstrapServers);

            var consumer = new Consumer<string, string>(cluster);
            await consumer.Subscribe(TopicName);

            var start = DateTime.UtcNow;
            while ((DateTime.UtcNow - start).TotalSeconds < 20)
            {
                var msg = await consumer.Consume();
                if (msg == null)
                {
                    await Task.Delay(100);
                    continue;
                }

                Console.WriteLine($"topic:{msg.Topic}:{msg.PartitionId}:{msg.Offset} - ({msg.Key}) {msg.Value}");
            }
        }

        private static ClusterManager GetClusterManager()
        {
            var serviceProvider =
                           new ServiceCollection()
                           .AddLogging(builder =>
                           {
                               builder.SetMinimumLevel(LogLevel.Warning);
                               builder.AddConsole();
                           })
                           .AddSingleton<KafkaConnectionFactory>()
                           .AddTransient<ClusterManager>()
                          .BuildServiceProvider();

            //return serviceProvider.GetRequiredService<ClusterManager>();
            return ActivatorUtilities.CreateInstance<ClusterManager>(serviceProvider, "a6k");
        }

        private static KafkaConnection GetKafka(int port = 29092)
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

            return new KafkaConnection(client, new IPEndPoint(IPAddress.Loopback, port), "fred");
        }

        private static async Task Fetch(KafkaConnection kafka, string topicName = TopicName, int partitionId = 0, int offset = 0)
        {
            Console.WriteLine($"----- Fetch: topic:{topicName} p:{partitionId} o:{offset}");

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
                        TopicName=topicName,
                        Partitions= new FetchRequest.Topic.Partition[]
                        {
                            new FetchRequest.Topic.Partition
                            {
                                PartitionId=partitionId,
                                CurrentLeaderEpoc=-1,
                                FetchOffset=offset,
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
                    Console.WriteLine($"  partition: {pr.PartitionId} error: {pr.ErrorCode.ToString()}");
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

        private static async Task FindCoordinator(KafkaConnection kafka)
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
        private static async Task JoinGroup(KafkaConnection kafka)
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

        private static async Task Produce(KafkaConnection kafka)
        {
            Console.WriteLine("----- Produce");

            var msg = new Message<string, string>
            {
                Topic = TopicName,
                Key = "fred",
                Value = "flintstone " + Guid.NewGuid().ToString()
            };
            Console.WriteLine(msg.Value);

            var response = await kafka.Produce(msg, IntrinsicSerializers.String, IntrinsicSerializers.String);
            Console.WriteLine($"throttle: {response.ThrottleTime}");
            foreach (var r in response.Topics)
            {
                Console.WriteLine($"topic: {r.Topic}");
                foreach (var p in r.Partitions)
                    Console.WriteLine($"    {p.Partition} (error: {p.ErrorCode.ToString()}) - {p.BaseOffset} {p.LogStartOffset} {Timestamp.UnixTimestampMsToDateTime(p.LogAppendTime)}");
            }
        }

        private static async Task GetVersion(KafkaConnection kafka)
        {
            Console.WriteLine("----- ApiVersion");
            var apiversions = await kafka.ApiVersion();
            Console.WriteLine("error: " + apiversions.ErrorCode);
            foreach (var v in apiversions.ApiVersions)
                Console.WriteLine($"{ApiKey.GetName(v.ApiKey).PadRight(20)} {v.MinVersion}->{v.MaxVersion}");
        }

        private static async Task GetMetadata(KafkaConnection kafka)
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
                Console.WriteLine($"    " + string.Join(", ", t.Partitions.Select(p => $"{ p.PartitionId}(l:{p.Leader} r:{(string.Join(",", p.Replicas))} isr:{(string.Join(",", p.Isr))})")));
            }
        }
    }
}
