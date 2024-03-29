﻿using System.Linq;
using NUnit.Framework;
using SimpleDB.DataStructures;

namespace SimpleDB.Test.DataStructures;

class RBTreeFindNodeEnumerableTest
{
    [Test]
    public void RBTreeFindNodeEnumerable()
    {
        var tree = new RBTree<int, int>();
        tree.InsertOrGetExists(1, default);
        var node2 = tree.InsertOrGetExists(2, default);
        var node3 = tree.InsertOrGetExists(3, default);
        var node4 = tree.InsertOrGetExists(4, default);
        tree.InsertOrGetExists(5, default);

        var result = new RBTreeFindNodeEnumerable<int, int>(tree.Root, 3).ToList();

        Assert.AreEqual(3, result.Count);

        Assert.AreEqual(node2, result[0].Node);
        Assert.AreEqual(false, result[0].ToLeft);
        Assert.AreEqual(true, result[0].ToRight);
        Assert.AreEqual(false, result[0].Finded);

        Assert.AreEqual(node4, result[1].Node);
        Assert.AreEqual(true, result[1].ToLeft);
        Assert.AreEqual(false, result[1].ToRight);
        Assert.AreEqual(false, result[1].Finded);

        Assert.AreEqual(node3, result[2].Node);
        Assert.AreEqual(false, result[2].ToLeft);
        Assert.AreEqual(false, result[2].ToRight);
        Assert.AreEqual(true, result[2].Finded);
    }
}
