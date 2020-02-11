using System.Buffers;
using System.Threading.Tasks;

namespace A6k.Kafka
{
    public interface ISerializer<T>
    {
        ValueTask WriteMessage(T message, IBufferWriter<byte> output);
    }
}
