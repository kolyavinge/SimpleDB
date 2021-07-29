using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Infrastructure
{
    internal static class EnumerableExt
    {
        public static void AddRange<T>(this ISet<T> set, IEnumerable<T> range)
        {
            foreach (var item in range)
            {
                set.Add(item);
            }
        }
    }
}
