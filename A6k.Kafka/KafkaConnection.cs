﻿using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Bedrock.Framework.Protocols;
using Microsoft.AspNetCore.Connections;

namespace A6k.Kafka
{
    public partial class KafkaConnection : IAsyncDisposable
    {
        private readonly ConnectionContext connection;
        private readonly string clientId;

        private int correlationId = 0;
        private BlockingCollection<Op> outbound = new BlockingCollection<Op>();
        private LinkedList<Op> inflight = new LinkedList<Op>();

        public KafkaConnection(ConnectionContext connection, string clientId)
        {
            this.connection = connection;
            this.clientId = clientId;

            _ = ProcessOutbound();
            _ = ProcessResponsesAsync();
        }

        private async Task<TResponse> SendRequest<TRequest, TResponse>(short apikey, short version, TRequest request, IMessageWriter<TRequest> messageWriter, IMessageReader<TResponse> messageReader)
        {
            var op = new Op<TRequest, TResponse>
            {
                CorrelationId = Interlocked.Increment(ref correlationId),
                ApiKey = apikey,
                Version = version,
                Request = request,
                MessageWriter = messageWriter,
                MessageReader = messageReader
            };
            outbound.Add(op);
            return await op.GetResponse();
        }

        private async Task ProcessOutbound(CancellationToken cancellationToken = default)
        {
            await Task.Yield();

            try
            {
                foreach (var op in outbound.GetConsumingEnumerable(cancellationToken))
                {
                    await SendRequest(op);
                }
            }
            catch (OperationCanceledException) { }
            finally
            {
            }

            async ValueTask SendRequest(Op op)
            {
                PushOp(op);

                var buffer = new MemoryBufferWriter<byte>();

                // write v1 Header
                buffer.WriteShort(op.ApiKey);
                buffer.WriteShort(op.Version);
                buffer.WriteInt(op.CorrelationId);
                buffer.WriteString(clientId);

                op.WriteMessage(buffer);

                connection.Transport.Output.WriteInt((int)buffer.Length);
                buffer.CopyTo(connection.Transport.Output);
                await connection.Transport.Output.FlushAsync().ConfigureAwait(false);
            }
        }

        private async ValueTask ProcessResponsesAsync()
        {
            await Task.Yield();
            var headerReader = new KafkaResponseHeaderReader();
            var reader = connection.CreateReader();

            while (true)
            {
                try
                {
                    var result = await reader.ReadAsync(headerReader);
                    var header = result.Message;

                    if (result.IsCompleted)
                        break;
                    reader.Advance();

                    var op = PopOp(header.CorrelationId);
                    if (op == null)
                        throw new InvalidOperationException("no outstanding op for correlationId: " + header.CorrelationId);

                    await op.ParseResponse(reader);
                }
                finally
                {
                    reader.Advance();
                }
            }
        }


        private void PushOp(Op op)
        {
            // haven't found a lockfree collection for this yet
            lock (inflight)
            {
                inflight.AddLast(op);
            }
        }
        private Op PopOp(int correctionId)
        {
            lock (inflight)
            {
                for (var node = inflight.First; node != null; node = node.Next)
                {
                    if (node.Value.CorrelationId == correctionId)
                    {
                        inflight.Remove(node);
                        return node.Value;
                    }
                }
            }
            return default;
        }

        public ValueTask DisposeAsync() => connection.DisposeAsync();

        private abstract class Op
        {
            public int CorrelationId { get; set; }
            public short ApiKey { get; set; }
            public short Version { get; set; }

            public abstract void WriteMessage(IBufferWriter<byte> output);
            public abstract ValueTask ParseResponse(ProtocolReader reader);
        }

        private class Op<TRequest, TResponse> : Op, IValueTaskSource<TResponse>
        {
            private ManualResetValueTaskSourceCore<TResponse> vts;

            public TRequest Request { get; set; }

            public IMessageWriter<TRequest> MessageWriter { get; set; }
            public IMessageReader<TResponse> MessageReader { get; set; }

            public override async ValueTask ParseResponse(ProtocolReader reader)
            {
                var result = await reader.ReadAsync(MessageReader);
                reader.Advance();
                vts.SetResult(result.Message);
            }

            public ValueTask<TResponse> GetResponse() => new ValueTask<TResponse>(this, vts.Version);

            public override void WriteMessage(IBufferWriter<byte> output) => MessageWriter?.WriteMessage(Request, output);

            TResponse IValueTaskSource<TResponse>.GetResult(short token) => vts.GetResult(token);
            ValueTaskSourceStatus IValueTaskSource<TResponse>.GetStatus(short token) => vts.GetStatus(token);
            void IValueTaskSource<TResponse>.OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags)
                => vts.OnCompleted(continuation, state, token, flags);
        }
    }
}