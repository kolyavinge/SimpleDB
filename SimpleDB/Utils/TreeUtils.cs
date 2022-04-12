using System;
using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Utils
{
    internal static class TreeUtils
    {
        public static IEnumerable<T> ToEnumerable<T>(T root, Func<T, T?> getLeftFunc, Func<T, T?> getRightFunc)
        {
            var stack = new Stack<T>();
            stack.Push(root);
            while (stack.Any())
            {
                var node = stack.Pop();
                yield return node;
                var left = getLeftFunc(node);
                if (left != null) stack.Push(left);
                var right = getRightFunc(node);
                if (right != null) stack.Push(right);
            }
        }
    }
}
