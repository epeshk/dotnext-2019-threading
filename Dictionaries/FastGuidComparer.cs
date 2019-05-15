using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Dictionaries
{
    public struct FastGuidComparer : IEqualityComparer<Guid>
    {
        public static readonly FastGuidComparer Instance = new FastGuidComparer();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe bool Equals(Guid x, Guid y)
        {
            var xptr = (long*) &x;
            var yptr = (long*) &y;

            return
                *xptr == *yptr && *(xptr + 1) == *(yptr + 1);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public unsafe int GetHashCode(Guid x)
        {
            var ptr = (int*)&x;

            int part1 = *ptr;
            int part2 = *(ptr + 1);
            int part3 = *(ptr + 2);
            int part4 = *(ptr + 3);

            return ((part1 ^ part2) * 397) ^ part3 ^ part4;
        }
    }
}