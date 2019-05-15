using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace Dictionaries
{
        /// <summary>
        /// <para>This class is a part of <see cref="StripedDictionary{TKey, TValue, TComparer}"/> and is not intended for standalone use.</para>
        /// <para>It relies on following assumptions about its usage:</para>
        /// <list type="bullet">
        ///     <item>All externally provided hash codes are positive.</item>
        ///     <item>All instances of <see cref="StripedDictionarySegment{TKey, TValue, TComparer}"/> must not have more than <see cref="short.MaxValue"/> elements.</item>
        ///     <item>Single writer + multiple readers threading model: all write access must be synchronized externally.</item>
        ///     <item>Low rate of writes (add/remove calls), especially removals.</item>
        /// </list>
        /// <para><see cref="StripedDictionarySegment{TKey, TValue, TComparer}"/> is a heavily rewritten <see cref="System.Collections.Generic.Dictionary{Guid, TValue}"/>. Key changes include:</para>
        /// <list type="bullet">
        ///     <item>Removal of all public methods except for <see cref="Insert"/>, <see cref="Remove"/>, <see cref="TryGetValue"/> and <see cref="GetEnumerator()"/>.</item>
        ///     <item>Internal arrays are now contained in single state object with atomic swap on resize.</item>
        ///     <item><see cref="TryGetValue"/> and <see cref="GetEnumerator"/> employ a trick from <see cref="System.Collections.Hashtable"/> to avoid struct tearing while reading entries.</item>
        ///     <item><see cref="TryGetValue"/> and <see cref="GetEnumerator"/> use a new algorithm of bucket stacks traversal to avoid consistency anomalies with per-bucket concurrency control.</item>
        ///     <item>Got rid of <see cref="System.Collections.Generic.IEqualityComparer{T}"/> in favor of direct calls to <see cref="FastGuidComparer"/>.</item>
        ///     <item>Internal indices are represented as <see cref="short"/> for lower memory utilization.</item>
        /// </list>
        /// </summary>
        internal class StripedDictionarySegment<TKey, TValue, TComparer> : IEnumerable<KeyValuePair<TKey, TValue>>
            where TComparer : struct, IEqualityComparer<TKey>
        {
            private static TComparer comparer = new TComparer();
            
            public static readonly int MaxCapacity;
            public static readonly int MaxCapacityBeforeLOH;
    
            private const int LargeObjectHeapThreshold = 85000;
    
            private const int DefaultCopyArraySize = 64;
    
            private const double GrowMultiplier = 1.5;
    
            private const int BucketsPerVersion = 4;
    
            private const int BucketVersionWriteMask = 1 << 31;
    
            private const int BucketVersionIdleMask = int.MaxValue;
    
            private const int HashCodesMask = 0x7FFFFFFF;
    
            private static readonly ArrayPool<KeyValuePair<TKey, TValue>> ArrayPool = ArrayPool<KeyValuePair<TKey, TValue>>.Shared;
    
            private volatile State state;
            private volatile short count;
            private volatile short freeList;
            private volatile short freeCount;
    
            [StructLayout(LayoutKind.Sequential, Pack = sizeof(short))]
            internal struct Entry
            {
                public TKey key;
    
                public TValue value;
    
                public short next;
            }
    
            private class State
            {
                public State(int size)
                {
                    if (size > short.MaxValue)
                        throw new OverflowException($"{nameof(StripedDictionarySegment<TKey, TValue, TComparer>)} can't expand to a capacity greater than {short.MaxValue} (requested {size}).");
    
                    entries = new Entry[size];
                    buckets = new short[size];
                    bucketVersions = new int[size / BucketsPerVersion + 1];
    
                    for (int i = 0; i < size; i++)
                        buckets[i] = -1;
                }
    
                public readonly Entry[] entries;
                public readonly short[] buckets;
                public readonly int[] bucketVersions;
            }
    
            static StripedDictionarySegment()
            {
                MaxCapacity = 1;
                MaxCapacityBeforeLOH = 1;
    
                foreach (var prime in HashHelpers.Primes)
                {
                    if (prime >= LargeObjectHeapThreshold)
                        break;
    
                    if (GC.GetGeneration(new Entry[prime]) != 0)
                        break;
    
                    MaxCapacityBeforeLOH = prime;
                }
    
                foreach (var prime in HashHelpers.Primes)
                {
                    if (prime > short.MaxValue)
                        break;
    
                    MaxCapacity = prime;
                }
            }
    
            public StripedDictionarySegment()
                : this(0)
            {
            }
    
            public StripedDictionarySegment(int capacity)
            {
                count = 0;
                freeCount = 0;
                freeList = -1;
    
                if (capacity < 0)
                    throw new ArgumentOutOfRangeException(nameof(capacity));
    
                if (capacity > 0)
                    Initialize(capacity);
            }
    
            public int Count => count - freeCount;
    
            public int Capacity => state?.entries?.Length ?? 0;
    
            #region Insert
    
            /// <summary>
            /// <para>Returns <c>true</c> if a new key/value pair was added.</para>
            /// <para>Returns <c>false</c> if <paramref name="canOverwrite"/> is <c>true</c> and an existing key/value pair was overwritten.</para>
            /// <para>Returns <c>false</c> if <paramref name="canOverwrite"/> is <c>false</c> and an existing key/value pair was encountered.</para>
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Insert(TKey key, TValue value, int hashCode, bool canOverwrite)
            {
                if (state == null)
                {
                    Initialize(0);
                }
    
                var bucket = hashCode % state.buckets.Length;
                var bucketVersionIndex = GetBucketVersionIndex(bucket);
    
                for (var i = state.buckets[bucket]; i >= 0; i = state.entries[i].next)
                {
                    if (comparer.Equals(state.entries[i].key, key))
                    {
                        if (canOverwrite)
                        {
                            MarkBucketForWriting(state, bucketVersionIndex);
    
                            state.entries[i].value = value;
    
                            UnmarkBucketForWriting(state, bucketVersionIndex);
                        }
    
                        return false;
                    }
                }
    
                int index;
                if (freeCount > 0)
                {
                    index = freeList;
                    freeList = state.entries[index].next;
                    freeCount--;
                }
                else
                {
                    if (count == state.entries.Length)
                    {
                        Resize();
                        bucket = hashCode % state.buckets.Length;
                        bucketVersionIndex = GetBucketVersionIndex(bucket);
                    }
                    index = count++;
                }
    
                MarkBucketForWriting(state, bucketVersionIndex);
    
                state.entries[index].next = state.buckets[bucket];
                state.entries[index].key = key;
                state.entries[index].value = value;
                state.buckets[bucket] = (short) index;
    
                UnmarkBucketForWriting(state, bucketVersionIndex);
    
                return true;
            }
    
            /// <summary>
            /// <para>This method exists to optimize resizing in <see cref="StripedGuidDictionary{T}"/>.</para>
            /// <para>It omits some of the code related to concurrency with readers and free list management.</para>
            /// <para>Caution: calls to this method must not contend with any read access!</para>
            /// <para>Caution: calls to this method must only be made with unique keys!</para>
            /// <para>Caution: calls to this method must only be made before any removals!</para>
            /// </summary>
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            internal void AddUnsafe(TKey key, TValue value, int hashCode)
            {
                if (state == null)
                {
                    Initialize(0);
                }
    
                var bucket = hashCode % state.buckets.Length;
    
                if (count == state.entries.Length)
                {
                    Resize();
                    bucket = hashCode % state.buckets.Length;
                }
    
                var index = count++;
    
                state.entries[index].next = state.buckets[bucket];
                state.entries[index].key = key;
                state.entries[index].value = value;
                state.buckets[bucket] = index;
            }
    
            #endregion
    
            #region Remove
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool Remove(TKey key, int hashCode)
            {
                if (state == null)
                    return false;
    
                var bucket = hashCode % state.buckets.Length;
                var bucketVersionIndex = GetBucketVersionIndex(bucket);
                var last = -1;
    
                for (var i = state.buckets[bucket]; i >= 0; last = i, i = state.entries[i].next)
                {
                    if (comparer.Equals(state.entries[i].key, key))
                    {
                        MarkBucketForWriting(state, bucketVersionIndex);
    
                        if (last < 0)
                        {
                            state.buckets[bucket] = state.entries[i].next;
                        }
                        else
                        {
                            state.entries[last].next = state.entries[i].next;
                        }
    
                        state.entries[i].next = freeList;
                        state.entries[i].key = default;
                        state.entries[i].value = default;
    
                        UnmarkBucketForWriting(state, bucketVersionIndex);
    
                        freeList = i;
                        freeCount++;
    
                        return true;
                    }
                }
    
                return false;
            }
    
            #endregion
    
            #region TryGetValue
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool TryGetValue(TKey key, int hashCode, out TValue value)
            {
                var currentState = state;
                if (currentState == null)
                {
                    value = default;
                    return false;
                }
    
                var bucket = hashCode % currentState.buckets.Length;
    
                if (!TrySearchInBucket(currentState, key, bucket, out var found, out value))
                {
                    var spinner = new SpinWait();
    
                    do
                    {
                        spinner.SpinOnce();
                    }
                    while (!TrySearchInBucket(currentState, key, bucket, out found, out value));
                }
    
                return found;
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool TrySearchInBucket(State state, TKey key, int bucket, out bool found, out TValue value)
            {
                // (iloktionov): This method attempts to search for an item by linearly traversing the corresponding bucket's stack.
                // (iloktionov): It may fail due to concurrent modification (adding/removal) of items in this bucket. 
                // (iloktionov): The only way to handle such a failure and obtain a "clean reading" is to retry.
    
                found = false;
                value = default;
    
                var bucketVersionIndex = GetBucketVersionIndex(bucket);
    
                if (IsMarkedForWriting(state, bucketVersionIndex, out var versionBefore))
                    return false;
    
                var index = state.buckets[bucket];
    
                while (index >= 0)
                {
                    if (!TryReadEntry(state, index, bucketVersionIndex, versionBefore, out var entry))
                        return false;
    
                    if (comparer.Equals(entry.key, key))
                    {
                        found = true;
                        value = entry.value;
                        break;
                    }
    
                    index = entry.next;
                }
    
                return Volatile.Read(ref state.bucketVersions[bucketVersionIndex]) == versionBefore;
            }
    
            #endregion
    
            #region Enumeration
    
            public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
            {
                var currentState = state;
                if (currentState == null)
                    yield break;
    
                var array = ArrayPool.Rent(DefaultCopyArraySize);
    
                try
                {
                    for (var bucket = 0; bucket < currentState.buckets.Length; bucket++)
                    {
                        if (!TryCopyBucket(currentState, bucket, array, out var copiedCount))
                        {
                            var spinner = new SpinWait();
    
                            do
                            {
                                if (copiedCount == array.Length)
                                {
                                    ArrayPool.Return(array);
    
                                    array = ArrayPool.Rent(array.Length * 2);
                                }
                                else
                                {
                                    spinner.SpinOnce();
                                }
                            }
                            while (!TryCopyBucket(currentState, bucket, array, out copiedCount));
                        }
    
                        for (var i = 0; i < copiedCount; i++)
                        {
                            yield return array[i];
                        }
                    }
                }
                finally
                {
                    ArrayPool.Return(array);
                }
            }
    
            IEnumerator IEnumerable.GetEnumerator()
            {
                return GetEnumerator();
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool TryCopyBucket(State state, int bucket, KeyValuePair<TKey, TValue>[] array, out int copiedCount)
            {
                // (iloktionov): This method attempts to copy all items in a bucket by linearly traversing the bucket's stack.
                // (iloktionov): It may fail due to concurrent modification (adding/removal) of items in this bucket. 
                // (iloktionov): The only way to handle such a failure and obtain a "clean reading" is to retry.
    
                copiedCount = 0;
    
                var bucketVersionIndex = GetBucketVersionIndex(bucket);
    
                if (IsMarkedForWriting(state, bucketVersionIndex, out var versionBefore))
                    return false;
    
                var index = state.buckets[bucket];
    
                while (index >= 0)
                {
                    if (!TryReadEntry(state, index, bucketVersionIndex, versionBefore, out var entry))
                        return false;
    
                    if (copiedCount == array.Length)
                        return false;
    
                    array[copiedCount++] = new KeyValuePair<TKey, TValue>(entry.key, entry.value);
    
                    index = entry.next;
                }
    
                return Volatile.Read(ref state.bucketVersions[bucketVersionIndex]) == versionBefore;
            }
    
            #endregion
    
            #region Capacity management
    
            private void Initialize(int capacity)
            {
                state = new State(HashHelpers.GetPrime(capacity));
            }
    
            private void Resize()
            {
                var newSize = HashHelpers.ExpandPrime(count, GrowMultiplier);
                if (newSize > MaxCapacityBeforeLOH && count < MaxCapacityBeforeLOH)
                    newSize = MaxCapacityBeforeLOH;
                if (newSize > MaxCapacity && count < MaxCapacity)
                    newSize = MaxCapacity;
    
                var newState = new State(newSize);
    
                var currentState = state;
                if (currentState.entries != null)
                {
                    Array.Copy(currentState.entries, 0, newState.entries, 0, count);
                }
    
                var newEntries = newState.entries;
                var newBuckets = newState.buckets;
    
                for (int i = 0; i < count; i++)
                {
                    var hash = comparer.GetHashCode(newEntries[i].key) & HashCodesMask;
                    var bucket = hash % newSize;
                    newEntries[i].next = newBuckets[bucket];
                    newBuckets[bucket] = (short)i;
                }
    
                state = newState;
            }
    
            #endregion
    
            #region Helper methods
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static int GetBucketVersionIndex(int bucket)
            {
                return bucket / BucketsPerVersion;
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void MarkBucketForWriting(State state, int versionIndex)
            {
                Volatile.Write(ref state.bucketVersions[versionIndex], Volatile.Read(ref state.bucketVersions[versionIndex]) | BucketVersionWriteMask);
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static void UnmarkBucketForWriting(State state, int versionIndex)
            {
                Volatile.Write(ref state.bucketVersions[versionIndex], ((Volatile.Read(ref state.bucketVersions[versionIndex]) & BucketVersionIdleMask) + 1) & int.MaxValue);
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool IsMarkedForWriting(State state, int versionIndex, out int version)
            {
                version = Volatile.Read(ref state.bucketVersions[versionIndex]);
    
                return (version & BucketVersionWriteMask) == BucketVersionWriteMask;
            }
    
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            private static bool TryReadEntry(State state, int index, int versionIndex, int versionBefore, out Entry entry)
            {
                entry = state.entries[index];
    
                return Volatile.Read(ref state.bucketVersions[versionIndex]) == versionBefore;
            }
    
            #endregion
        }
}