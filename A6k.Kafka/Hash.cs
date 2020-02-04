﻿using System;
using System.Buffers;

namespace A6k.Kafka
{
    public static class Hash
    {
        public static class Murmur2
        {
            public static uint Compute(in ReadOnlySpan<byte> input)
            {
                const uint seed = 0xc58f1a7a;

                const uint m = 0x5bd1e995;
                const int r = 24;

                int length = input.Length;
                if (length == 0)
                    return 0;
                uint h = seed ^ (uint)length;
                int currentIndex = 0;
                while (length >= 4)
                {
                    uint k = (uint)(input[currentIndex++] | input[currentIndex++] << 8 | input[currentIndex++] << 16 | input[currentIndex++] << 24);
                    k *= m;
                    k ^= k >> r;
                    k *= m;

                    h *= m;
                    h ^= k;
                    length -= 4;
                }
                switch (length)
                {
                    case 3:
                        h ^= (ushort)(input[currentIndex++] | input[currentIndex++] << 8);
                        h ^= (uint)(input[currentIndex] << 16);
                        h *= m;
                        break;
                    case 2:
                        h ^= (ushort)(input[currentIndex++] | input[currentIndex] << 8);
                        h *= m;
                        break;
                    case 1:
                        h ^= input[currentIndex];
                        h *= m;
                        break;
                    default:
                        break;
                }

                h ^= h >> 13;
                h *= m;
                h ^= h >> 15;

                return h;
            }

            public static uint Compute(ReadOnlySequence<byte> input)
            {
                if (input.IsSingleSegment)
                    return Compute(input.First.Span);
                else
                    // TODO: hash from sequence
                    return Compute(input.ToArray());
            }

        }

        public static Crc32Hash Crc32 => Crc32Hash.Crc32;
        public static Crc32Hash Crc32C => Crc32Hash.Crc32C;

        public sealed class Crc32Hash
        {
            public static readonly Crc32Hash Crc32 = new Crc32Hash(0x04C11DB7u);
            public static readonly Crc32Hash Crc32C = new Crc32Hash(0x82F63B78u);

            private readonly uint[] _table = new uint[16 * 256];

            private Crc32Hash(uint poly)
            {
                var table = _table;
                for (uint i = 0; i < 256; i++)
                {
                    uint res = i;
                    for (int t = 0; t < 16; t++)
                    {
                        for (int k = 0; k < 8; k++) res = (res & 1) == 1 ? poly ^ (res >> 1) : (res >> 1);
                        table[(t * 256) + i] = res;
                    }
                }
            }

            public uint Compute(ReadOnlySequence<byte> input, uint crc = 0)
            {
                foreach (var part in input)
                    crc = Compute(part.Span, crc);

                return crc;
            }

            public uint Compute(ReadOnlySpan<byte> input, uint crc = 0)
            {
                uint crcLocal = uint.MaxValue ^ crc;
                int offset = 0;
                int length = input.Length;

                uint[] table = _table;
                while (length >= 16)
                {
                    var a = table[(3 * 256) + input[offset + 12]]
                        ^ table[(2 * 256) + input[offset + 13]]
                        ^ table[(1 * 256) + input[offset + 14]]
                        ^ table[(0 * 256) + input[offset + 15]];

                    var b = table[(7 * 256) + input[offset + 8]]
                        ^ table[(6 * 256) + input[offset + 9]]
                        ^ table[(5 * 256) + input[offset + 10]]
                        ^ table[(4 * 256) + input[offset + 11]];

                    var c = table[(11 * 256) + input[offset + 4]]
                        ^ table[(10 * 256) + input[offset + 5]]
                        ^ table[(9 * 256) + input[offset + 6]]
                        ^ table[(8 * 256) + input[offset + 7]];

                    var d = table[(15 * 256) + ((byte)crcLocal ^ input[offset])]
                        ^ table[(14 * 256) + ((byte)(crcLocal >> 8) ^ input[offset + 1])]
                        ^ table[(13 * 256) + ((byte)(crcLocal >> 16) ^ input[offset + 2])]
                        ^ table[(12 * 256) + ((crcLocal >> 24) ^ input[offset + 3])];

                    crcLocal = d ^ c ^ b ^ a;
                    offset += 16;
                    length -= 16;
                }

                while (--length >= 0)
                    crcLocal = table[(byte)(crcLocal ^ input[offset++])] ^ crcLocal >> 8;

                return crcLocal ^ uint.MaxValue;
            }
        }
    }
}