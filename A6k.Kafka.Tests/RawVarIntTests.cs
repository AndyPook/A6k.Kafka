using System.Buffers;
using Xunit;

namespace A6k.Kafka.Tests
{
    public class RawVarIntTests
    {
        // see https://developers.google.com/protocol-buffers/docs/encoding#varints
        
        [Fact]
        public void Write_RawVarInt32_1()
        {
            // arrange
            var buffer = new ArrayBufferWriter<byte>();

            // act
            buffer.WriteRawVarInt(1);

            // assert
            var mem = buffer.WrittenMemory;

            // 0000 00001
            Assert.Equal(1, mem.Length);
            Assert.Equal((byte)1, mem.Span[0]);
        }

        [Fact]
        public void Write_RawVarInt32_300()
        {
            // arrange
            var buffer = new ArrayBufferWriter<byte>();

            // act
            buffer.WriteVarInt(300);

            // assert
            var mem = buffer.WrittenMemory;

            // 1010 1100 0000 0010
            Assert.Equal(2, mem.Length);
            Assert.Equal((byte)0b1010_1100, mem.Span[0]);
            Assert.Equal((byte)0b0000_0010, mem.Span[1]);
        }

        [Fact]
        public void Read_RawVarInt32_1()
        {
            // arrange
            var buffer = new ArrayBufferWriter<byte>();

            // act
            // 0000 0001
            buffer.Write(new byte[] { 0b0000_0001 });

            // assert
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            Assert.True(reader.TryReadVarint32(out var value));
            Assert.Equal(1, value);
        }

        [Fact]
        public void Read_RawVarInt32_300()
        {
            // arrange
            var buffer = new ArrayBufferWriter<byte>();

            // act
            // 1010 1100 0000 0010
            buffer.Write(new byte[] { 0b1010_1100, 0b0000_0010 });

            // assert
            var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(buffer.WrittenMemory));
            Assert.True(reader.TryReadVarint32(out var value));
            Assert.Equal(300, value);
        }
    }
}
