using System;
using System.Collections.Generic;
using NUnit.Framework;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.DataStructures
{
    class IntRBTreeSerializer : IRBTreeNodeSerializer<int, int>
    {
        public void SerializeKey(int nodeKey, IWriteableStream stream) { stream.WriteInt(nodeKey); }
        public int DeserializeKey(IReadableStream stream) { return stream.ReadInt(); }
        public void SerializeValue(int nodeValue, IWriteableStream stream) { stream.WriteInt(nodeValue); }
        public int DeserializeValue(IReadableStream stream) { return stream.ReadInt(); }
    }

    class RBTreeSerializerTest
    {
        private MemoryFileStream _stream;
        private RBTree<int, int> _tree;
        private RBTreeSerializer<int, int> _serializer;

        [SetUp]
        public void Setup()
        {
            _stream = new MemoryFileStream();
            _tree = new RBTree<int, int>();
            _serializer = new RBTreeSerializer<int, int>(new IntRBTreeSerializer());
        }

        [Test]
        public void SerializeDeserialize_EmptyTree()
        {
            _serializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = _serializer.Deserialize(_stream);
            Assert.AreEqual(null, result.Root);
        }

        [Test]
        public void SerializeDeserialize_One()
        {
            _tree.Insert(new RBTree<int, int>.Node(1) { Value = 1 });
            _serializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = _serializer.Deserialize(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_Two()
        {
            _tree.Insert(new RBTree<int, int>.Node(1) { Value = 1 });
            _tree.Insert(new RBTree<int, int>.Node(2) { Value = 2 });
            _serializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = _serializer.Deserialize(_stream);
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
            _serializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = _serializer.Deserialize(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_100()
        {
            for (int i = 0; i < 100; i++) _tree.Insert(new RBTree<int, int>.Node(i) { Value = i });
            _serializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = _serializer.Deserialize(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        [Test]
        public void SerializeDeserialize_1000()
        {
            for (int i = 0; i < 1000; i++) _tree.Insert(new RBTree<int, int>.Node(i) { Value = i });
            _serializer.Serialize(_tree, _stream);
            _stream.Seek(0, System.IO.SeekOrigin.Begin);
            var result = _serializer.Deserialize(_stream);
            var originalNodes = _tree.Root.GetAllChildren();
            var resultNodes = result.Root.GetAllChildren();
            TreesAreEquals(originalNodes, resultNodes);
        }

        private void TreesAreEquals<TKey, TValue>(List<RBTree<TKey, TValue>.Node> originalNodes, List<RBTree<TKey, TValue>.Node> resultNodes) where TKey : IComparable<TKey>
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
    }
}
