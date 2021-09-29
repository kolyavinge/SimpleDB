using System;
using System.Collections.Generic;

namespace SimpleDB.Utils.EnumerableExtension
{
    internal static class EnumerableExt
    {
        public static void Each<T>(this IEnumerable<T> collection, Action<T> action)
        {
            foreach (var item in collection)
            {
                action(item);
            }
        }

        public static void AddRange<T>(this ISet<T> set, IEnumerable<T> range)
        {
            foreach (var item in range)
            {
                set.Add(item);
            }
        }

        public static void RemoveRange<T>(this ISet<T> set, IEnumerable<T> range)
        {
            foreach (var item in range)
            {
                set.Remove(item);
            }
        }
    }
}
