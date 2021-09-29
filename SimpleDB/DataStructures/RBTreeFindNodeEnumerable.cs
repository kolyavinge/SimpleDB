using System;
using System.Collections;
using System.Collections.Generic;

namespace SimpleDB.DataStructures
{
    internal class RBTreeFindNodeEnumerable<TKey, TValue> : IEnumerable<RBTreeFindNodeEnumerable<TKey, TValue>.StepResult> where TKey : IComparable<TKey>
    {
        private readonly RBTree<TKey, TValue>.Node _root;
        private readonly TKey _keyToFind;

        public class StepResult
        {
            public RBTree<TKey, TValue>.Node Node;
            public bool ToLeft;
            public bool ToRight;
            public bool Finded;
        }

        public RBTreeFindNodeEnumerable(RBTree<TKey, TValue>.Node root, TKey keyToFind)
        {
            _root = root;
            _keyToFind = keyToFind;
        }

        public IEnumerator<StepResult> GetEnumerator()
        {
            var node = _root;
            while (node != null)
            {
                var compareResult = _keyToFind.CompareTo(node.Key);
                if (compareResult < 0)
                {
                    yield return new StepResult { Node = node, ToLeft = true };
                    node = node.Left;
                }
                else if (compareResult > 0)
                {
                    yield return new StepResult { Node = node, ToRight = true };
                    node = node.Right;
                }
                else
                {
                    yield return new StepResult { Node = node, Finded = true };
                    yield break;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
