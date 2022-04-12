using System;
using System.Collections.Generic;

namespace SimpleDB.DataStructures
{
    internal class RBTree<TKey, TValue> where TKey : IComparable<TKey>
    {
        public enum Color : byte { Red = 1, Black = 2 }

        public sealed class Node
        {
            public readonly TKey Key;
            public TValue? Value;
            public Color Color;
            public Node? Parent;
            public Node? Left;
            public Node? Right;

            public Node(TKey key)
            {
                Key = key;
                Color = Color.Red;
            }

            public List<Node> GetAllNodesAsc()
            {
                var result = new List<Node>();
                GetAllNodesAscRec(this, result);
                return result;
            }

            public List<Node> GetAllNodesDesc()
            {
                var result = new List<Node>();
                GetAllNodesDescRec(this, result);
                return result;
            }

            private void GetAllNodesAscRec(Node node, List<Node> result)
            {
                if (node.Left != null) GetAllNodesAscRec(node.Left, result);
                result.Add(node);
                if (node.Right != null) GetAllNodesAscRec(node.Right, result);
            }

            private void GetAllNodesDescRec(Node node, List<Node> result)
            {
                if (node.Right != null) GetAllNodesDescRec(node.Right, result);
                result.Add(node);
                if (node.Left != null) GetAllNodesDescRec(node.Left, result);
            }
        }

        private static readonly Node DummyNode = new(default) { Color = Color.Black };

        public RBTree() { }

        public RBTree(Node root)
        {
            Root = root;
        }

        public Node? Root { get; private set; }

        public void Clear()
        {
            Root = null;
        }

        public Node? Find(TKey key)
        {
            var node = Root;
            while (node != null)
            {
                var compareResult = key.CompareTo(node.Key);
                if (compareResult < 0) node = node.Left;
                else if (compareResult > 0) node = node.Right;
                else return node;
            }

            return null;
        }

        public Node InsertOrGetExists(TKey key)
        {
            if (Root != null) return NodeInsertOrGetExists(key);
            else return RootInsert(key);
        }

        private Node RootInsert(TKey key)
        {
            return Root = new Node(key) { Color = Color.Black };
        }

        private Node NodeInsertOrGetExists(TKey key)
        {
            Node node;
            var parent = Root!;
            while (true)
            {
                var compareResult = key.CompareTo(parent.Key);
                if (compareResult < 0)
                {
                    if (parent.Left != null) parent = parent.Left;
                    else
                    {
                        node = new Node(key);
                        parent.Left = node;
                        node.Parent = parent;
                        break;
                    }
                }
                else if (compareResult > 0)
                {
                    if (parent.Right != null) parent = parent.Right;
                    else
                    {
                        node = new Node(key);
                        parent.Right = node;
                        node.Parent = parent;
                        break;
                    }
                }
                else return parent;
            }

            InsertFixup(node);

            return node;
        }

        private void InsertFixup(Node node)
        {
            var grandParent = GetGrandParent(node);
            if (grandParent != null && node.Parent!.Color == Color.Red)
            {
                var nodeLeftChild = IsLeftChild(node);
                var nodeRightChild = IsRightChild(node);
                var parentLeftChild = IsLeftChild(node.Parent);
                var parentRightChild = IsRightChild(node.Parent);
                // uncle = red
                if (parentRightChild && grandParent.Left != null && grandParent.Left.Color == Color.Red ||
                    parentLeftChild && grandParent.Right != null && grandParent.Right.Color == Color.Red)
                {
                    grandParent.Color = Color.Red;
                    grandParent.Left!.Color = Color.Black;
                    grandParent.Right!.Color = Color.Black;
                    InsertFixup(grandParent);
                }
                // uncle = black (triangle)
                else if (nodeLeftChild && parentRightChild &&
                    (grandParent.Left == null || grandParent.Left.Color == Color.Black))
                {
                    RightRotate(node.Parent);
                    LeftRotate(grandParent);
                    node.Color = Color.Black;
                    grandParent.Color = Color.Red;
                }
                // uncle = black (triangle)
                else if (nodeRightChild && parentLeftChild &&
                    (grandParent.Right == null || grandParent.Right.Color == Color.Black))
                {
                    LeftRotate(node.Parent);
                    RightRotate(grandParent);
                    node.Color = Color.Black;
                    grandParent.Color = Color.Red;
                }
                // uncle = black (line)
                else if (nodeLeftChild && parentLeftChild &&
                    (grandParent.Right == null || grandParent.Right.Color == Color.Black))
                {
                    var originalParent = node.Parent;
                    RightRotate(grandParent);
                    originalParent.Color = Color.Black;
                    grandParent.Color = Color.Red;
                }
                // uncle = black (line)
                else if (nodeRightChild && parentRightChild &&
                    (grandParent.Left == null || grandParent.Left.Color == Color.Black))
                {
                    var originalParent = node.Parent;
                    LeftRotate(grandParent);
                    originalParent.Color = Color.Black;
                    grandParent.Color = Color.Red;
                }
            }
            else if (node == Root)
            {
                node.Color = Color.Black;
            }
        }

        public Node? Delete(TKey key)
        {
            var deleted = Find(key);
            if (deleted == null) return null;
            Node replacement, x;
            // no children
            if (deleted.Left == null && deleted.Right == null)
            {
                replacement = DummyNode;
                x = replacement;
            }
            // one child
            else if (deleted.Left == null || deleted.Right == null)
            {
                replacement = deleted.Left ?? deleted.Right ?? throw new RBTreeException();
                x = replacement;
            }
            // two children
            else
            {
                replacement = GetHighestNode(deleted.Left);
                x = replacement.Left ?? DummyNode;
                ReplaceDeletedNode(replacement, x);
            }
            ReplaceDeletedNode(deleted, replacement);
            if (deleted.Color == Color.Black)
            {
                if (replacement.Color == Color.Red) replacement.Color = Color.Black;
                else DeleteFixup(x);
            }
            if (x != replacement) DeleteDummyIfNeeded(x);
            DeleteDummyIfNeeded(replacement);
            deleted.Parent = null;
            deleted.Left = null;
            deleted.Right = null;

            return deleted;
        }

        private void DeleteDummyIfNeeded(Node dummy)
        {
            if (dummy == DummyNode && dummy.Parent != null)
            {
                if (IsLeftChild(dummy)) dummy.Parent.Left = null;
                else dummy.Parent.Right = null;
                dummy.Parent = null;
            }
        }

        private void DeleteFixup(Node node)
        {
            // case 1
            if (node.Parent == null)
            {
                if (Root == DummyNode) Root = null;
                return;
            }
            // case 2
            var sibling = GetSibling(node);
            if (sibling.Color == Color.Red)
            {
                node.Parent.Color = Color.Red;
                sibling.Color = Color.Black;
                if (IsLeftChild(node)) LeftRotate(node.Parent);
                else RightRotate(node.Parent);
                sibling = GetSibling(node);
            }
            // case 3
            if (node.Parent.Color == Color.Black &&
                sibling.Color == Color.Black &&
                (sibling.Left == null || sibling.Left.Color == Color.Black) &&
                (sibling.Right == null || sibling.Right.Color == Color.Black))
            {
                sibling.Color = Color.Red;
                DeleteFixup(node.Parent);
            }
            // case 4
            else if (node.Parent.Color == Color.Red &&
                sibling.Color == Color.Black &&
                (sibling.Left == null || sibling.Left.Color == Color.Black) &&
                (sibling.Right == null || sibling.Right.Color == Color.Black))
            {
                sibling.Color = Color.Red;
                node.Parent.Color = Color.Black;
            }
            // case 5
            else if (sibling.Color == Color.Black)
            {
                if (IsLeftChild(node) &&
                    (sibling.Right == null || sibling.Right.Color == Color.Black) &&
                    sibling.Left!.Color == Color.Red)
                {
                    sibling.Color = Color.Red;
                    sibling.Left.Color = Color.Black;
                    RightRotate(sibling);
                    sibling = GetSibling(node);
                }
                else if (IsRightChild(node) &&
                    (sibling.Left == null || sibling.Left.Color == Color.Black) &&
                    sibling.Right!.Color == Color.Red)
                {
                    sibling.Color = Color.Red;
                    sibling.Right.Color = Color.Black;
                    LeftRotate(sibling);
                    sibling = GetSibling(node);
                }
                // case 6
                sibling.Color = node.Parent.Color;
                node.Parent.Color = Color.Black;
                if (IsLeftChild(node))
                {
                    if (sibling.Right != null) sibling.Right.Color = Color.Black;
                    LeftRotate(node.Parent);
                }
                else
                {
                    if (sibling.Left != null) sibling.Left.Color = Color.Black;
                    RightRotate(node.Parent);
                }
            }
        }

        private void ReplaceDeletedNode(Node node, Node replacement)
        {
            replacement.Parent = node.Parent;
            if (node.Left != replacement && node.Left != null)
            {
                replacement.Left = node.Left;
                replacement.Left.Parent = replacement;
            }
            else
            {
                replacement.Left = null;
            }
            if (node.Right != replacement && node.Right != null)
            {
                replacement.Right = node.Right;
                replacement.Right.Parent = replacement;
            }
            else
            {
                replacement.Right = null;
            }
            if (IsLeftChild(node)) node.Parent!.Left = replacement;
            else if (IsRightChild(node)) node.Parent!.Right = replacement;
            if (node == Root) Root = replacement;
        }

        private Node GetHighestNode(Node node)
        {
            while (node.Right != null) node = node.Right;
            return node;
        }

        private void LeftRotate(Node rotated)
        {
            var newParent = rotated.Right ?? throw new RBTreeException();
            rotated.Right = newParent.Left;
            if (newParent.Left != null) newParent.Left.Parent = rotated;
            newParent.Parent = rotated.Parent;
            if (rotated != Root)
            {
                if (IsLeftChild(rotated)) rotated.Parent!.Left = newParent;
                else rotated.Parent!.Right = newParent;
            }
            else
            {
                Root = newParent;
            }
            newParent.Left = rotated;
            rotated.Parent = newParent;
        }

        private void RightRotate(Node rotated)
        {
            var newParent = rotated.Left ?? throw new RBTreeException();
            rotated.Left = newParent.Right;
            if (newParent.Right != null) newParent.Right.Parent = rotated;
            newParent.Parent = rotated.Parent;
            if (rotated != Root)
            {
                if (IsRightChild(rotated)) rotated.Parent!.Right = newParent;
                else rotated.Parent!.Left = newParent;
            }
            else
            {
                Root = newParent;
            }
            newParent.Right = rotated;
            rotated.Parent = newParent;
        }

        private bool IsLeftChild(Node node) { return node.Parent != null && node.Parent.Left == node; }

        private bool IsRightChild(Node node) { return node.Parent != null && node.Parent.Right == node; }

        private Node? GetGrandParent(Node node) { return node.Parent != null && node.Parent.Parent != null ? node.Parent.Parent : null; }

        private Node GetSibling(Node node)
        {
            if (node.Parent != null)
            {
                if (IsLeftChild(node)) return node.Parent.Right ?? throw new RBTreeException();
                else return node.Parent.Left ?? throw new RBTreeException();
            }

            throw new RBTreeException();
        }
    }

    internal class RBTreeException : Exception
    {
    }
}
