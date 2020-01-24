using System.Buffers;

namespace A6k.Kafka
{
    public interface ISerializer<T>
    {
        void WriteMessage(T message, IBufferWriter<byte> output);
    }
}
