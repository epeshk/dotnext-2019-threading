// original by iloktionov

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Dictionaries
{ 
    /// <summary>
    /// <para><see cref="StripedDictionary{TKey,TValue,TComparer}"/> is a narrowly specialized replacement for <see cref="System.Collections.Concurrent.ConcurrentDictionary{T1, T2}"/> with these key features:</para>
    /// <list type="bullet">
    ///     <item>Single writer + multiple readers threading model: all write access must be synchronized externally, but any amount of readers are safe to work concurrently with a writer.</item>
    ///     <item>No object-per-item overhead, leading to a small amount of references for GC to crawl.</item>
    ///     <item>No internal arrays large enough to end up in large object heap (LOH).</item>
    ///     <item>Significantly smaller overall memory footprint.</item>
    ///     <item>Significantly faster write operations.</item>
    ///     <item>Automatic shrinking after massive removal of elements.</item>
    /// </list>
    /// <para>It was designed with a single purpose: to contain millions of key-value pairs in a GC-friendly way.</para>
    /// </summary>
    public class StripedDictionary<TKey, TValue, TComparer> : IEnumerable<KeyValuePair<TKey, TValue>>
        where TComparer : struct, IEqualityComparer<TKey>
    {
        private const int MinimumSegmentsCount = 7;
        private const int MinimumSegmentCapacity = 16;

        private static readonly int OptimalSegmentCapacity = (int)(StripedDictionarySegment<TKey, TValue, TComparer>.MaxCapacityBeforeLOH * 0.9);

        private const double GrowMultiplier = 1.75;
        private const double ShrinkThreshold = 0.40;
        private const double ShrinkMultiplier = 0.50;

        private const int HashCodesMask = 0x7FFFFFFF;

        private const int MaximumResizeAttempts = 3;

        private const int MinimumAddsBetweenFailedResizes = 1000;

        private volatile StripedDictionarySegment<TKey, TValue, TComparer>[] segments;
        private volatile int count;
        private volatile int capacity;
        private volatile int resizeCooldown;

        public StripedDictionary(int capacity = 128)
        {
            Initialize(capacity);
        }

        public int Count => count;

        public int Capacity => capacity;

        public int SegmentsCount => segments.Length;

        public bool HasArraysInLOH => segments.Any(s => s.Capacity > StripedDictionarySegment<TKey, TValue, TComparer>.MaxCapacityBeforeLOH);

        #region Add, Set

        public bool TryAdd(TKey key, TValue value)
        {
            return Insert(key, value, false);
        }

        public void Add(TKey key, TValue value)
        {
            if (!Insert(key, value, false))
                throw new ArgumentException($"Element with key '{key}' already exists.");
        }

        public void Set(TKey key, TValue value)
        {
            Insert(key, value, true);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool Insert(TKey key, TValue value, bool canOverwrite)
        {
            ComputeCoordinates(segments, key, out var hash, out var index);

            var segment = segments[index];
            var addedNew = segment.Insert(key, value, hash, canOverwrite);
            if (addedNew)
            {
                count++;

                if (NeedToResizeAfterAdding(segment))
                {
                    ResizeAfterAdding();
                }
            }

            return addedNew;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NeedToResizeAfterAdding(StripedDictionarySegment<TKey, TValue, TComparer> segment)
        {
            if (segment.Count < StripedDictionarySegment<TKey, TValue, TComparer>.MaxCapacityBeforeLOH)
                return false;

            if (resizeCooldown <= 0)
                return true;

            return --resizeCooldown <= 0;
        }

        private void ResizeAfterAdding()
        {
            var currentCapacity = capacity;

            for (var i = 0; i < MaximumResizeAttempts; i++)
            {
                if (Resize(currentCapacity = (int)(currentCapacity * GrowMultiplier)))
                    return;
            }

            resizeCooldown = MinimumAddsBetweenFailedResizes;
        }

        #endregion

        #region Remove

        public bool Remove(TKey key)
        {
            ComputeCoordinates(segments, key, out var hash, out var segmentIndex);

            var removed = segments[segmentIndex].Remove(key, hash);
            if (removed)
            {
                count--;

                if (NeedToResizeAfterRemoving())
                {
                    ResizeAfterRemoving();
                }
            }

            return removed;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private bool NeedToResizeAfterRemoving()
        {
            return segments.Length > MinimumSegmentsCount && count <= capacity * ShrinkThreshold;
        }

        private void ResizeAfterRemoving()
        {
            Resize((int)(capacity * ShrinkMultiplier));
        }

        #endregion

        #region Search

        public bool TryGetValue(TKey key, out TValue value)
        {
            var currentSegments = segments;

            ComputeCoordinates(currentSegments, key, out var hash, out var segmentIndex);

            if (currentSegments[segmentIndex].TryGetValue(key, hash, out value))
                return true;

            value = default;
            return false;
        }

        public bool ContainsKey(TKey key)
        {
            var currentSegments = segments;

            ComputeCoordinates(currentSegments, key, out var hash, out var segmentIndex);

            return currentSegments[segmentIndex].TryGetValue(key, hash, out _);
        }

        #endregion

        #region Indexer

        public TValue this[TKey key]
        {
            get
            {
                if (TryGetValue(key, out var value))
                    return value;

                throw new KeyNotFoundException($"Key '{key}' is not present.");
            }
            set => Set(key, value);
        }

        #endregion

        #region Enumeration

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            var currentSegments = segments;

            foreach (var segment in currentSegments)
            {
                foreach (var key in segment)
                {
                    yield return new KeyValuePair<TKey, TValue>(key.Key, key.Value);
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Capacity management

        private void Initialize(int desiredCapacity)
        {
            var segmentsCount = GetOptimalSegmentsCount(desiredCapacity);
            var initialSegmentCapacity = GetOptimalSegmentCapacity(desiredCapacity, segmentsCount);

            segments = new StripedDictionarySegment<TKey, TValue, TComparer>[segmentsCount];

            for (int i = 0; i < segmentsCount; i++)
            {
                segments[i] = new StripedDictionarySegment<TKey, TValue, TComparer>(initialSegmentCapacity);
            }

            capacity = segmentsCount * StripedDictionarySegment<TKey, TValue, TComparer>.MaxCapacityBeforeLOH;
        }

        private bool Resize(int desiredCapacity)
        {
            var segmentsCount = GetOptimalSegmentsCount(desiredCapacity);
            if (segmentsCount == segments.Length)
                return true;

            var initialSegmentCapacity = GetOptimalSegmentCapacity(desiredCapacity, segmentsCount);

            var newSegments = new StripedDictionarySegment<TKey, TValue, TComparer>[segmentsCount];

            for (int i = 0; i < segmentsCount; i++)
            {
                newSegments[i] = new StripedDictionarySegment<TKey, TValue, TComparer>(initialSegmentCapacity);
            }

            foreach (var segment in segments)
            {
                foreach (var pair in segment)
                {
                    ComputeCoordinates(newSegments, pair.Key, out var hash, out var segmentIndex);

                    var newSegment = newSegments[segmentIndex];

                    newSegment.AddUnsafe(pair.Key, pair.Value, hash);

                    if (newSegment.Count == StripedDictionarySegment<TKey, TValue, TComparer>.MaxCapacity)
                        return false;
                }
            }

            segments = newSegments;

            capacity = segmentsCount * StripedDictionarySegment<TKey, TValue, TComparer>.MaxCapacityBeforeLOH;

            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetOptimalSegmentsCount(int capacity)
        {
            return Math.Max(MinimumSegmentsCount, HashHelpers.GetPrime(capacity / OptimalSegmentCapacity));
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int GetOptimalSegmentCapacity(int capacity, int segmentsCount)
        {
            return Math.Max(MinimumSegmentCapacity, capacity / segmentsCount);
        }

        #endregion

        #region Helper methods for segment navigation

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void ComputeCoordinates(
            StripedDictionarySegment<TKey, TValue, TComparer>[] segments,
            TKey key,
            out int hashCode,
            out int segmentIndex)
        {
            hashCode = default(TComparer).GetHashCode(key) & HashCodesMask;

            segmentIndex = hashCode % segments.Length;
        }

        #endregion
    }
}