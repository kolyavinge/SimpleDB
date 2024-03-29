﻿using System;
using System.Collections.Generic;
using NUnit.Framework;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;
using SimpleDB.Test.Tools;

namespace SimpleDB.Test.DataStructures;

class IntRBTreeSerializerDeserializer : IRBTreeNodeSerializer<int, int>, IRBTreeNodeDeserializer<int, int>
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
    private RBTreeDeserializer<int, int> _deserializer;

    [SetUp]
    public void Setup()
    {
        _stream = new MemoryFileStream();
        _tree = new RBTree<int, int>();
        _serializer = new RBTreeSerializer<int, int>(new IntRBTreeSerializerDeserializer());
        _deserializer = new RBTreeDeserializer<int, int>(new IntRBTreeSerializerDeserializer());
    }

    [Test]
    public void SerializeDeserialize_EmptyTree()
    {
        _serializer.Serialize(_tree, _stream);
        _stream.Seek(0, System.IO.SeekOrigin.Begin);
        var result = _deserializer.Deserialize(_stream);
        Assert.AreEqual(null, result.Root);
    }

    [Test]
    public void SerializeDeserialize_One()
    {
        _tree.InsertOrGetExists(1, default).Value = 1;
        _serializer.Serialize(_tree, _stream);
        _stream.Seek(0, System.IO.SeekOrigin.Begin);
        var result = _deserializer.Deserialize(_stream);
        var originalNodes = _tree.Root.GetAllNodesAsc();
        var resultNodes = result.Root.GetAllNodesAsc();
        TreesAreEquals(originalNodes, resultNodes);
    }

    [Test]
    public void SerializeDeserialize_Two()
    {
        _tree.InsertOrGetExists(1, default).Value = 1;
        _tree.InsertOrGetExists(2, default).Value = 2;
        _serializer.Serialize(_tree, _stream);
        _stream.Seek(0, System.IO.SeekOrigin.Begin);
        var result = _deserializer.Deserialize(_stream);
        var originalNodes = _tree.Root.GetAllNodesAsc();
        var resultNodes = result.Root.GetAllNodesAsc();
        TreesAreEquals(originalNodes, resultNodes);
    }

    [Test]
    public void SerializeDeserialize_Three()
    {
        _tree.InsertOrGetExists(1, default).Value = 1;
        _tree.InsertOrGetExists(2, default).Value = 2;
        _tree.InsertOrGetExists(3, default).Value = 3;
        _serializer.Serialize(_tree, _stream);
        _stream.Seek(0, System.IO.SeekOrigin.Begin);
        var result = _deserializer.Deserialize(_stream);
        var originalNodes = _tree.Root.GetAllNodesAsc();
        var resultNodes = result.Root.GetAllNodesAsc();
        TreesAreEquals(originalNodes, resultNodes);
    }

    [Test]
    public void SerializeDeserialize_100()
    {
        for (int i = 0; i < 100; i++) _tree.InsertOrGetExists(i, default).Value = i;
        _serializer.Serialize(_tree, _stream);
        _stream.Seek(0, System.IO.SeekOrigin.Begin);
        var result = _deserializer.Deserialize(_stream);
        var originalNodes = _tree.Root.GetAllNodesAsc();
        var resultNodes = result.Root.GetAllNodesAsc();
        TreesAreEquals(originalNodes, resultNodes);
    }

    [Test]
    public void SerializeDeserialize_1000()
    {
        for (int i = 0; i < 1000; i++) _tree.InsertOrGetExists(i, default).Value = i;
        _serializer.Serialize(_tree, _stream);
        _stream.Seek(0, System.IO.SeekOrigin.Begin);
        var result = _deserializer.Deserialize(_stream);
        var originalNodes = _tree.Root.GetAllNodesAsc();
        var resultNodes = result.Root.GetAllNodesAsc();
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
            if (originalNodes[i].Left is not null)
            {
                Assert.AreEqual(originalNodes[i].Left.Key, resultNodes[i].Left.Key);
            }
            else
            {
                Assert.AreEqual(null, resultNodes[i].Left);
            }
            if (originalNodes[i].Right is not null)
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
