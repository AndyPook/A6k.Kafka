using System;
using System.Buffers;
using Xunit;

namespace A6k.Kafka.Tests
{
    public class VarIntTests
    {
        // see https://developers.google.com/protocol-buffers/docs/encoding#varints
        // https://developers.google.com/protocol-buffers/docs/encoding#signed-integers

        [Theory]
        [InlineData(0, 0)]
        [InlineData(-1, 1)]
        [InlineData(1, 2)]
        [InlineData(-2, 3)]
        public void Write_VarInt32(int x, byte expected)
        {
            // arrange
            var buffer = new ArrayBufferWriter<byte>();

            // act
            buffer.WriteVarInt(x);

            // assert
            var mem = buffer.WrittenMemory;

            Assert.Equal(1, mem.Length);
            Assert.Equal(expected, mem.Span[0]);
        }

        [Theory]
        [InlineData(2147483647, 4294967294)]
        [InlineData(-2147483648, 4294967295)]
        public void Write_VarInt32_large(int x, uint expected)
        {
            // arrange
            var buffer = new ArrayBufferWriter<byte>();

            // act
            buffer.WriteVarInt(x);

            // assert
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            Assert.True(reader.TryReadRawVarint32(out var value));
            Assert.Equal(expected, value);
        }
    }
}
