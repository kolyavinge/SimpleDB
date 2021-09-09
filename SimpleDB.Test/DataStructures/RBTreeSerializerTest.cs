using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using NUnit.Framework;
using SimpleDB.DataStructures;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.DataStructures
{
    class RBTreeSerializerTest
    {
        private MemoryFileStream _stream;
        private RBTree<int, int> _tree;

        [SetUp]
        public void Setup()
        {
            _stream = new MemoryFileStream();
            _tree = new RBTree<int, int>();
        }

        [Test]
        public void SerializeDeserialize_EmptyTree()
        {
            RBTreeSerializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<int, int>(_stream);
            Assert.AreEqual(null, result.Root);
        }

        [Test]
        public void SerializeDeserialize_One()
        {
            _tree.Insert(new RBTree<int, int>.Node(1) { Value = 1 });
            RBTreeSerializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<int, int>(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_Two()
        {
            _tree.Insert(new RBTree<int, int>.Node(1) { Value = 1 });
            _tree.Insert(new RBTree<int, int>.Node(2) { Value = 2 });
            RBTreeSerializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<int, int>(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_Three()
        {
            _tree.Insert(new RBTree<int, int>.Node(1) { Value = 1 });
            _tree.Insert(new RBTree<int, int>.Node(2) { Value = 2 });
            _tree.Insert(new RBTree<int, int>.Node(3) { Value = 3 });
            RBTreeSerializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<int, int>(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_100()
        {
            for (int i = 0; i < 100; i++) _tree.Insert(new RBTree<int, int>.Node(i) { Value = i });
            RBTreeSerializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<int, int>(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_1000()
        {
            for (int i = 0; i < 1000; i++) _tree.Insert(new RBTree<int, int>.Node(i) { Value = i });
            RBTreeSerializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<int, int>(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_KeyString_1000()
        {
            var tree = new RBTree<string, string>();
            for (int i = 0; i < 1000; i++) tree.Insert(new RBTree<string, string>.Node(i.ToString()) { Value = i.ToString() });
            RBTreeSerializer.Serialize(tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<string, string>(_stream);
            var originalNodes = tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        class TestKey : IComparable<TestKey>
        {
            public string Value { get; set; }

            public int CompareTo([AllowNull] TestKey x)
            {
                return Value.CompareTo(x.Value);
            }
        }

        [Test]
        public void SerializeDeserialize_KeyObject_1000()
        {
            var tree = new RBTree<TestKey, string>();
            for (int i = 0; i < 1000; i++) tree.Insert(new RBTree<TestKey, string>.Node(new TestKey { Value = i.ToString() }) { Value = i.ToString() });
            RBTreeSerializer.Serialize(tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = RBTreeSerializer.Deserialize<TestKey, string>(_stream);
            var originalNodes = tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        private void TreesAreEquals(List<RBTree<int, int>.Node> originalNodes, List<RBTree<int, int>.Node> resultNodes)
        {
            Assert.AreEqual(originalNodes.Count, resultNodes.Count);
            for (int i = 0; i < originalNodes.Count; i++)
            {
                Assert.AreEqual(originalNodes[i].Key, resultNodes[i].Key);
                Assert.AreEqual(originalNodes[i].Color, resultNodes[i].Color);
                Assert.AreEqual(originalNodes[i].Value, resultNodes[i].Value);
                if (originalNodes[i].Left != null)
                {
                    Assert.AreEqual(originalNodes[i].Left.Key, resultNodes[i].Left.Key);
                }
                else
                {
                    Assert.AreEqual(null, resultNodes[i].Left);
                }
                if (originalNodes[i].Right != null)
                {
                    Assert.AreEqual(originalNodes[i].Right.Key, resultNodes[i].Right.Key);
                }
                else
                {
                    Assert.AreEqual(null, resultNodes[i].Right);
                }
            }
        }

        private void TreesAreEquals(List<RBTree<string, string>.Node> originalNodes, List<RBTree<string, string>.Node> resultNodes)
        {
            Assert.AreEqual(originalNodes.Count, resultNodes.Count);
            for (int i = 0; i < originalNodes.Count; i++)
            {
                Assert.AreEqual(originalNodes[i].Key, resultNodes[i].Key);
                Assert.AreEqual(originalNodes[i].Color, resultNodes[i].Color);
                Assert.AreEqual(originalNodes[i].Value, resultNodes[i].Value);
                if (originalNodes[i].Left != null)
                {
                    Assert.AreEqual(originalNodes[i].Left.Key, resultNodes[i].Left.Key);
                }
                else
                {
                    Assert.AreEqual(null, resultNodes[i].Left);
                }
                if (originalNodes[i].Right != null)
                {
                    Assert.AreEqual(originalNodes[i].Right.Key, resultNodes[i].Right.Key);
                }
                else
                {
                    Assert.AreEqual(null, resultNodes[i].Right);
                }
            }
        }

        private void TreesAreEquals(List<RBTree<TestKey, string>.Node> originalNodes, List<RBTree<TestKey, string>.Node> resultNodes)
        {
            Assert.AreEqual(originalNodes.Count, resultNodes.Count);
            for (int i = 0; i < originalNodes.Count; i++)
            {
                Assert.AreEqual(originalNodes[i].Key.Value, resultNodes[i].Key.Value);
                Assert.AreEqual(originalNodes[i].Color, resultNodes[i].Color);
                Assert.AreEqual(originalNodes[i].Value, resultNodes[i].Value);
                if (originalNodes[i].Left != null)
                {
                    Assert.AreEqual(originalNodes[i].Left.Key.Value, resultNodes[i].Left.Key.Value);
                }
                else
                {
                    Assert.AreEqual(null, resultNodes[i].Left);
                }
                if (originalNodes[i].Right != null)
                {
                    Assert.AreEqual(originalNodes[i].Right.Key.Value, resultNodes[i].Right.Key.Value);
                }
                else
                {
                    Assert.AreEqual(null, resultNodes[i].Right);
                }
            }
        }
    }
}
