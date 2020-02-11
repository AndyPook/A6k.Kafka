using System;
using System.Threading.Tasks;

namespace A6k.Kafka
{
    public interface IDeserializer<T>
    {
        ValueTask<T> Deserialize(in ReadOnlySpan<byte> input);
    }
}
