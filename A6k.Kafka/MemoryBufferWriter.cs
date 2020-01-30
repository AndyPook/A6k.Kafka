using System;
using System.Buffers;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace A6k.Kafka
{
    /// <summary>
    /// Manages a sequence of elements, readily castable as a <see cref="ReadOnlySequence{T}"/>.
    /// </summary>
    /// <remarks>
    /// Instance members are not thread-safe.
    /// </remarks>
    [DebuggerDisplay("{" + nameof(DebuggerDisplay) + ",nq}")]
    public class MemoryBufferWriter : IBufferWriter<byte>, IDisposable
    {
        private const int DefaultBufferSize = 4 * 1024;

        private readonly Stack<SequenceSegment> segmentPool = new Stack<SequenceSegment>();

        private readonly MemoryPool<byte> memoryPool;

        private SequenceSegment first;

        private SequenceSegment last;

        /// <summary>
        /// Initializes a new instance of the <see cref="Sequence"/> class.
        /// </summary>
        /// <param name="memoryPool">The pool to use for recycling backing arrays.</param>
        public MemoryBufferWriter(MemoryPool<byte> memoryPool = null)
        {
            this.memoryPool = memoryPool ?? MemoryPool<byte>.Shared;
        }

        /// <summary>
        /// Gets this sequence expressed as a <see cref="ReadOnlySequence{T}"/>.
        /// </summary>
        /// <returns>A read only sequence representing the data in this object.</returns>
        public ReadOnlySequence<byte> AsReadOnlySequence => this;

        /// <summary>
        /// Gets the length of the sequence.
        /// </summary>
        public long Length => AsReadOnlySequence.Length;

        public void CopyTo(IBufferWriter<byte> output)
        {
            // copy buffer to output
            // Try to minimize segments in the target writer by hinting at the total size.
            output.GetSpan((int)Length);
            foreach (var segment in AsReadOnlySequence)
                output.Write(segment.Span);
        }

        /// <summary>
        /// Gets the value to display in a debugger datatip.
        /// </summary>
        private string DebuggerDisplay => $"Length: {AsReadOnlySequence.Length}";

        /// <summary>
        /// Expresses this sequence as a <see cref="ReadOnlySequence{T}"/>.
        /// </summary>
        /// <param name="sequence">The sequence to convert.</param>
        public static implicit operator ReadOnlySequence<byte>(MemoryBufferWriter sequence)
        {
            return sequence.first != null
                ? new ReadOnlySequence<byte>(sequence.first, sequence.first.Start, sequence.last, sequence.last.End)
                : ReadOnlySequence<byte>.Empty;
        }

        /// <summary>
        /// Removes all elements from the sequence from its beginning to the specified position,
        /// considering that data to have been fully processed.
        /// </summary>
        /// <param name="position">
        /// The position of the first element that has not yet been processed.
        /// This is typically <see cref="ReadOnlySequence{T}.End"/> after reading all elements from that instance.
        /// </param>
        public void AdvanceTo(SequencePosition position)
        {
            var firstSegment = (SequenceSegment)position.GetObject();
            int firstIndex = position.GetInteger();

            // Before making any mutations, confirm that the block specified belongs to this sequence.
            var current = first;
            while (current != firstSegment && current != null)
                current = current.Next;

            if (current == null)
                throw new ArgumentException("Position does not represent a valid position in this sequence.", nameof(position));

            // Also confirm that the position is not a prior position in the block.
            if (firstIndex < current.Start)
                throw new ArgumentException("Position must not be earlier than current position.", nameof(position));

            // Now repeat the loop, performing the mutations.
            current = first;
            while (current != firstSegment)
            {
                var next = current.Next;
                current.ResetMemory();
                current = next;
            }

            firstSegment.AdvanceTo(firstIndex);

            if (firstSegment.Length == 0)
                firstSegment = RecycleAndGetNext(firstSegment);

            first = firstSegment;

            if (first == null)
                last = null;
        }

        /// <summary>
        /// Advances the sequence to include the specified number of elements initialized into memory
        /// returned by a prior call to <see cref="GetMemory(int)"/>.
        /// </summary>
        /// <param name="count">The number of elements written into memory.</param>
        public void Advance(int count)
        {
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count), "Value must be greater than or equal to 0");

            last.End += count;
        }

        /// <summary>
        /// Gets writable memory that can be initialized and added to the sequence via a subsequent call to <see cref="Advance(int)"/>.
        /// </summary>
        /// <param name="sizeHint">The size of the memory required, or 0 to just get a convenient (non-empty) buffer.</param>
        /// <returns>The requested memory.</returns>
        public Memory<byte> GetMemory(int sizeHint)
        {
            if (sizeHint < 0)
                throw new ArgumentOutOfRangeException(nameof(sizeHint), "Value for must be greater than or equal to 0");

            if (sizeHint == 0)
            {
                if (last?.WritableBytes > 0)
                    sizeHint = last.WritableBytes;
                else
                    sizeHint = DefaultBufferSize;
            }

            if (last == null || last.WritableBytes < sizeHint)
                Append(memoryPool.Rent(Math.Min(sizeHint, memoryPool.MaxBufferSize)));

            return last.TrailingSlack;
        }

        /// <summary>
        /// Gets writable memory that can be initialized and added to the sequence via a subsequent call to <see cref="Advance(int)"/>.
        /// </summary>
        /// <param name="sizeHint">The size of the memory required, or 0 to just get a convenient (non-empty) buffer.</param>
        /// <returns>The requested memory.</returns>
        public Span<byte> GetSpan(int sizeHint) => GetMemory(sizeHint).Span;

        /// <summary>
        /// Clears the entire sequence, recycles associated memory into pools,
        /// and resets this instance for reuse.
        /// This invalidates any <see cref="ReadOnlySequence{T}"/> previously produced by this instance.
        /// </summary>
        [EditorBrowsable(EditorBrowsableState.Never)]
        public void Dispose() => Reset();

        /// <summary>
        /// Clears the entire sequence and recycles associated memory into pools.
        /// This invalidates any <see cref="ReadOnlySequence{T}"/> previously produced by this instance.
        /// </summary>
        public void Reset()
        {
            var current = first;
            while (current != null)
                current = RecycleAndGetNext(current);

            first = last = null;
        }

        private void Append(IMemoryOwner<byte> array)
        {
            if (array == null)
                throw new ArgumentNullException(nameof(array));

            var segment = segmentPool.Count > 0 ? segmentPool.Pop() : new SequenceSegment();
            segment.SetMemory(array, 0, 0);

            if (last == null)
                first = last = segment;
            else
            {
                if (last.Length > 0)
                {
                    // Add a new block.
                    last.SetNext(segment);
                }
                else
                {
                    // The last block is completely unused. Replace it instead of appending to it.
                    var current = first;
                    if (first != last)
                    {
                        while (current.Next != last)
                            current = current.Next;
                    }
                    else
                        first = segment;

                    current.SetNext(segment);
                    RecycleAndGetNext(last);
                }

                last = segment;
            }
        }

        private SequenceSegment RecycleAndGetNext(SequenceSegment segment)
        {
            var recycledSegment = segment;
            segment = segment.Next;
            recycledSegment.ResetMemory();
            segmentPool.Push(recycledSegment);
            return segment;
        }

        private class SequenceSegment : ReadOnlySequenceSegment<byte>
        {
            /// <summary>
            /// Backing field for the <see cref="End"/> property.
            /// </summary>
            private int end;

            /// <summary>
            /// Gets the index of the first element in <see cref="AvailableMemory"/> to consider part of the sequence.
            /// </summary>
            /// <remarks>
            /// The <see cref="Start"/> represents the offset into <see cref="AvailableMemory"/> where the range of "active" bytes begins. At the point when the block is leased
            /// the <see cref="Start"/> is guaranteed to be equal to 0. The value of <see cref="Start"/> may be assigned anywhere between 0 and
            /// <see cref="AvailableMemory"/>.Length, and must be equal to or less than <see cref="End"/>.
            /// </remarks>
            internal int Start { get; private set; }

            /// <summary>
            /// Gets or sets the index of the element just beyond the end in <see cref="AvailableMemory"/> to consider part of the sequence.
            /// </summary>
            /// <remarks>
            /// The <see cref="End"/> represents the offset into <see cref="AvailableMemory"/> where the range of "active" bytes ends. At the point when the block is leased
            /// the <see cref="End"/> is guaranteed to be equal to <see cref="Start"/>. The value of <see cref="Start"/> may be assigned anywhere between 0 and
            /// <see cref="AvailableMemory"/>.Length, and must be equal to or less than <see cref="End"/>.
            /// </remarks>
            internal int End
            {
                get => end;
                set
                {
                    if (value > AvailableMemory.Length)
                        throw new ArgumentOutOfRangeException(nameof(value), "Value must be less than or equal to AvailableMemory.Length");

                    end = value;

                    // If we ever support creating these instances on existing arrays, such that
                    // Start isn't 0 at the beginning, we'll have to "pin" Start and remove
                    // Advance, forcing Sequence<byte> itself to track it, the way Pipe does it internally.
                    Memory = AvailableMemory.Slice(0, value);
                }
            }

            internal Memory<byte> TrailingSlack => AvailableMemory.Slice(End);

            internal IMemoryOwner<byte> MemoryOwner { get; private set; }

            internal Memory<byte> AvailableMemory { get; private set; }

            internal int Length => End - Start;

            /// <summary>
            /// Gets the amount of writable bytes in this segment.
            /// It is the amount of bytes between <see cref="Length"/> and <see cref="End"/>.
            /// </summary>
            internal int WritableBytes
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get => AvailableMemory.Length - End;
            }

            internal new SequenceSegment Next
            {
                get => (SequenceSegment)base.Next;
                set => base.Next = value;
            }

            internal void SetMemory(IMemoryOwner<byte> memoryOwner)
            {
                SetMemory(memoryOwner, 0, memoryOwner.Memory.Length);
            }

            internal void SetMemory(IMemoryOwner<byte> memoryOwner, int start, int end)
            {
                MemoryOwner = memoryOwner;

                AvailableMemory = MemoryOwner.Memory;

                RunningIndex = 0;
                Start = start;
                End = end;
                Next = null;
            }

            internal void ResetMemory()
            {
                MemoryOwner.Dispose();
                MemoryOwner = null;
                AvailableMemory = default;

                Memory = default;
                Next = null;
                Start = 0;
                end = 0;
            }

            internal void SetNext(SequenceSegment segment)
            {
                if (segment == null)
                    throw new ArgumentNullException(nameof(segment));

                Next = segment;
                segment.RunningIndex = RunningIndex + End;
            }

            internal void AdvanceTo(int offset)
            {
                if (offset > End)
                    throw new ArgumentOutOfRangeException(nameof(offset));

                Start = offset;
            }
        }
    }
}