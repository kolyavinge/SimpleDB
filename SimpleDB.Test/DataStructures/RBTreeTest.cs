using System;
using System.Linq;
using NUnit.Framework;
using SimpleDB.DataStructures;

namespace SimpleDB.Test.DataStructures;

public class RBTreeTest
{
    private RBTree<int, int> _tree;

    [SetUp]
    public void Setup()
    {
        _tree = new RBTree<int, int>();
    }

    [Test]
    public void Insert_Root()
    {
        _tree.InsertOrGetExists(1);
        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: -", AsString(_tree.Root));
    }

    [Test]
    public void Insert_LeftChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: -", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: -", AsString(node5));
        Assert.AreEqual("key: 1, color: Red, parent: 5, left: -, right: -", AsString(node1));
    }

    [Test]
    public void Insert_RightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node10 = _tree.InsertOrGetExists(10);

        Assert.AreEqual("key: 5, color: Black, parent: -, left: -, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: -, right: 10", AsString(node5));
        Assert.AreEqual("key: 10, color: Red, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Insert_LeftRightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(node5));
        Assert.AreEqual("key: 1, color: Red, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 10, color: Red, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Insert_Fixup_UncleRed_1()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node2 = _tree.InsertOrGetExists(2);

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: 2", AsString(node1));
        Assert.AreEqual("key: 10, color: Black, parent: 5, left: -, right: -", AsString(node10));
        Assert.AreEqual("key: 2, color: Red, parent: 1, left: -, right: -", AsString(node2));
    }

    [Test]
    public void Insert_Fixup_UncleRed_2()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node9 = _tree.InsertOrGetExists(9);

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 10, color: Black, parent: 5, left: 9, right: -", AsString(node10));
        Assert.AreEqual("key: 9, color: Red, parent: 10, left: -, right: -", AsString(node9));
    }

    [Test]
    public void Insert_Fixup_UncleBlackTriangle_RightLeftRotation()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        node1.Color = RBTree<int, int>.Color.Black;
        var node9 = _tree.InsertOrGetExists(9);

        Assert.AreEqual("key: 9, color: Black, parent: -, left: 5, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 9, color: Black, parent: -, left: 5, right: 10", AsString(node9));
        Assert.AreEqual("key: 5, color: Red, parent: 9, left: 1, right: -", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 10, color: Red, parent: 9, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Insert_Fixup_UncleBlackTriangle_LeftRightRotation()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        node10.Color = RBTree<int, int>.Color.Black;
        var node2 = _tree.InsertOrGetExists(2);

        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(_tree.Root));
        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(node2));
        Assert.AreEqual("key: 1, color: Red, parent: 2, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 5, color: Red, parent: 2, left: -, right: 10", AsString(node5));
        Assert.AreEqual("key: 10, color: Black, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Insert_Fixup_UncleBlackLine_LeftRotation()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node9 = _tree.InsertOrGetExists(9);
        node1.Color = RBTree<int, int>.Color.Black;
        var node10 = _tree.InsertOrGetExists(10);

        Assert.AreEqual("key: 9, color: Black, parent: -, left: 5, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 9, color: Black, parent: -, left: 5, right: 10", AsString(node9));
        Assert.AreEqual("key: 5, color: Red, parent: 9, left: 1, right: -", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 10, color: Red, parent: 9, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Insert_Fixup_UncleBlackLine_RightRotation()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node2 = _tree.InsertOrGetExists(2);
        var node10 = _tree.InsertOrGetExists(10);
        node10.Color = RBTree<int, int>.Color.Black;
        var node1 = _tree.InsertOrGetExists(1);

        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(_tree.Root));
        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(node2));
        Assert.AreEqual("key: 1, color: Red, parent: 2, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 5, color: Red, parent: 2, left: -, right: 10", AsString(node5));
        Assert.AreEqual("key: 10, color: Black, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Insert_1Twice_SameNode()
    {
        var node = _tree.InsertOrGetExists(1);
        var result = _tree.InsertOrGetExists(1);
        Assert.AreEqual(node, result);
    }

    [Test]
    public void Insert_20()
    {
        var node1 = _tree.InsertOrGetExists(1);
        var node2 = _tree.InsertOrGetExists(2);
        var node3 = _tree.InsertOrGetExists(3);
        var node4 = _tree.InsertOrGetExists(4);
        var node5 = _tree.InsertOrGetExists(5);
        var node6 = _tree.InsertOrGetExists(6);
        var node7 = _tree.InsertOrGetExists(7);
        var node8 = _tree.InsertOrGetExists(8);
        var node9 = _tree.InsertOrGetExists(9);
        var node10 = _tree.InsertOrGetExists(10);
        var node11 = _tree.InsertOrGetExists(11);
        var node12 = _tree.InsertOrGetExists(12);
        var node13 = _tree.InsertOrGetExists(13);
        var node14 = _tree.InsertOrGetExists(14);
        var node15 = _tree.InsertOrGetExists(15);
        var node16 = _tree.InsertOrGetExists(16);
        var node17 = _tree.InsertOrGetExists(17);
        var node18 = _tree.InsertOrGetExists(18);
        var node19 = _tree.InsertOrGetExists(19);
        var node20 = _tree.InsertOrGetExists(20);

        // 1
        Assert.AreEqual(null, node8.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node8.Color);

        // 2
        Assert.AreEqual(node8, node4.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Red, node4.Color);

        Assert.AreEqual(node8, node12.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Red, node12.Color);

        // 3
        Assert.AreEqual(node4, node2.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node2.Color);

        Assert.AreEqual(node4, node6.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node6.Color);

        Assert.AreEqual(node12, node10.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node10.Color);

        Assert.AreEqual(node12, node16.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node16.Color);

        // 4
        Assert.AreEqual(node2, node1.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node1.Color);

        Assert.AreEqual(node2, node3.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node3.Color);

        Assert.AreEqual(node6, node5.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node5.Color);

        Assert.AreEqual(node6, node7.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node7.Color);

        Assert.AreEqual(node10, node9.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node9.Color);

        Assert.AreEqual(node10, node11.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node11.Color);

        Assert.AreEqual(node16, node14.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Red, node14.Color);

        Assert.AreEqual(node16, node18.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Red, node18.Color);

        // 5
        Assert.AreEqual(node14, node13.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node13.Color);

        Assert.AreEqual(node14, node15.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node15.Color);

        Assert.AreEqual(node18, node17.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node17.Color);

        Assert.AreEqual(node18, node19.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Black, node19.Color);

        // 6
        Assert.AreEqual(node19, node20.Parent);
        Assert.AreEqual(RBTree<int, int>.Color.Red, node20.Color);
    }

    [Test]
    public void Find_1()
    {
        _tree.InsertOrGetExists(1);
        var node = _tree.Find(1);
        Assert.AreEqual(1, node.Key);
    }

    [Test]
    public void Find_2()
    {
        _tree.InsertOrGetExists(5);
        _tree.InsertOrGetExists(1);
        _tree.InsertOrGetExists(10);

        var node = _tree.Find(10);
        Assert.AreEqual(10, node.Key);

        node = _tree.Find(1);
        Assert.AreEqual(1, node.Key);

        node = _tree.Find(5);
        Assert.AreEqual(5, node.Key);
    }

    [Test]
    public void Find_No()
    {
        _tree.InsertOrGetExists(5);
        _tree.InsertOrGetExists(1);
        _tree.InsertOrGetExists(10);

        var node = _tree.Find(123);
        Assert.AreEqual(null, node);
    }

    [Test]
    public void Find_EmptyTree()
    {
        var node = _tree.Find(123);
        Assert.AreEqual(null, node);
    }

    [Test]
    public void Find_BigTree()
    {
        var count = 1000;
        for (int i = 0; i < count; i++)
        {
            _tree.InsertOrGetExists(i);
        }
        Assert.AreEqual(count, _tree.Root.GetAllNodesAsc().Count());
        for (int i = 0; i < count; i++)
        {
            var node = _tree.Find(i);
            Assert.AreEqual(i, node.Key);
        }
    }

    [Test]
    public void GetAllNodesAsc()
    {
        var count = 1000;
        int i = 0;
        while (i < count) _tree.InsertOrGetExists(i++);
        i = 0;
        foreach (var node in _tree.Root.GetAllNodesAsc())
        {
            Assert.AreEqual(i++, node.Key);
        }
    }

    [Test]
    public void Clear()
    {
        var count = 1000;
        for (int i = 0; i < count; i++) _tree.InsertOrGetExists(i);

        _tree.Clear();
        Assert.AreEqual(null, _tree.Root);

        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(node5));
        Assert.AreEqual("key: 1, color: Red, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 10, color: Red, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_Root()
    {
        _tree.InsertOrGetExists(5);
        _tree.Delete(5);
        Assert.AreEqual(null, _tree.Root);
    }

    [Test]
    public void Delete_AllNodes()
    {
        _tree.InsertOrGetExists(1);
        _tree.InsertOrGetExists(2);
        _tree.InsertOrGetExists(3);
        _tree.InsertOrGetExists(4);
        _tree.InsertOrGetExists(5);
        _tree.Delete(1);
        _tree.Delete(2);
        _tree.Delete(3);
        _tree.Delete(4);
        _tree.Delete(5);
        Assert.AreEqual(null, _tree.Root);
    }

    [Test]
    public void Delete_NoNode()
    {
        _tree.InsertOrGetExists(5);
        _tree.InsertOrGetExists(1);
        _tree.InsertOrGetExists(10);

        var node = _tree.Delete(123);
        Assert.AreEqual(null, node);
    }

    [Test]
    public void Delete_NoChildren_Left()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);

        _tree.Delete(1);

        Assert.AreEqual("key: 1, color: Red, parent: -, left: -, right: -", AsString(node1));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: -, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: -, right: 10", AsString(node5));
        Assert.AreEqual("key: 10, color: Red, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_NoChildren_Right()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Red, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: -", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: -", AsString(node5));
        Assert.AreEqual("key: 1, color: Red, parent: 5, left: -, right: -", AsString(node1));
    }

    [Test]
    public void Delete_LeftChildWithOneLeftChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node0 = _tree.InsertOrGetExists(0);
        node1.Color = RBTree<int, int>.Color.Black;
        node0.Color = RBTree<int, int>.Color.Red;

        _tree.Delete(1);

        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: -", AsString(node1));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 0, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 0, right: 10", AsString(node5));
        Assert.AreEqual("key: 0, color: Black, parent: 5, left: -, right: -", AsString(node0));
        Assert.AreEqual("key: 10, color: Black, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_RightChildWithOneLeftChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node9 = _tree.InsertOrGetExists(9);
        node10.Color = RBTree<int, int>.Color.Black;
        node9.Color = RBTree<int, int>.Color.Red;

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Black, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 9", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 9", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 9, color: Black, parent: 5, left: -, right: -", AsString(node9));
    }

    [Test]
    public void Delete_LeftChildWithOneRightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node2 = _tree.InsertOrGetExists(2);
        node1.Color = RBTree<int, int>.Color.Black;
        node2.Color = RBTree<int, int>.Color.Red;

        _tree.Delete(1);

        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: -", AsString(node1));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 2, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 2, right: 10", AsString(node5));
        Assert.AreEqual("key: 2, color: Black, parent: 5, left: -, right: -", AsString(node2));
        Assert.AreEqual("key: 10, color: Black, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_RightChildWithOneRightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node11 = _tree.InsertOrGetExists(11);
        node10.Color = RBTree<int, int>.Color.Black;
        node11.Color = RBTree<int, int>.Color.Red;

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Black, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 11", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 11", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 11, color: Black, parent: 5, left: -, right: -", AsString(node11));
    }

    [Test]
    public void Delete_LeftChildWithTwoChildren()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node2 = _tree.InsertOrGetExists(2);
        var node3 = _tree.InsertOrGetExists(3);

        _tree.Delete(2);

        Assert.AreEqual("key: 2, color: Black, parent: -, left: -, right: -", AsString(node2));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 10", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: 3", AsString(node1));
        Assert.AreEqual("key: 3, color: Red, parent: 1, left: -, right: -", AsString(node3));
        Assert.AreEqual("key: 10, color: Black, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_RightChildWithTwoChildren()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node9 = _tree.InsertOrGetExists(9);
        var node11 = _tree.InsertOrGetExists(11);

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Black, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 9", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: 9", AsString(node5));
        Assert.AreEqual("key: 1, color: Black, parent: 5, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 9, color: Black, parent: 5, left: -, right: 11", AsString(node9));
        Assert.AreEqual("key: 11, color: Red, parent: 9, left: -, right: -", AsString(node11));
    }

    [Test]
    public void Delete_Case_1_DeleteLastNode()
    {
        _tree.InsertOrGetExists(5);
        _tree.Delete(5);
        Assert.AreEqual(null, _tree.Root);

        var node5 = _tree.InsertOrGetExists(5);
        Assert.AreEqual(node5, _tree.Root);
    }

    [Test]
    public void Delete_Case_1()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node3 = _tree.InsertOrGetExists(3);
        node1.Color = RBTree<int, int>.Color.Black;
        node5.Color = RBTree<int, int>.Color.Black;

        _tree.Delete(3);

        Assert.AreEqual("key: 3, color: Black, parent: -, left: -, right: -", AsString(node3));

        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: 5", AsString(_tree.Root));
        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: 5", AsString(node1));
        Assert.AreEqual("key: 5, color: Red, parent: 1, left: -, right: -", AsString(node5));
    }

    [Test]
    public void Delete_Case_2_4_LeftChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node11 = _tree.InsertOrGetExists(11);
        var node12 = _tree.InsertOrGetExists(12);
        node11.Color = RBTree<int, int>.Color.Red;
        node10.Color = RBTree<int, int>.Color.Black;
        node12.Color = RBTree<int, int>.Color.Black;

        _tree.Delete(1);

        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: -", AsString(node1));

        Assert.AreEqual("key: 11, color: Black, parent: -, left: 5, right: 12", AsString(_tree.Root));
        Assert.AreEqual("key: 11, color: Black, parent: -, left: 5, right: 12", AsString(node11));
        Assert.AreEqual("key: 5, color: Black, parent: 11, left: -, right: 10", AsString(node5));
        Assert.AreEqual("key: 12, color: Black, parent: 11, left: -, right: -", AsString(node12));
        Assert.AreEqual("key: 10, color: Red, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_Case_2_4_RightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node2 = _tree.InsertOrGetExists(2);
        var node10 = _tree.InsertOrGetExists(10);
        var node1 = _tree.InsertOrGetExists(1);
        var node3 = _tree.InsertOrGetExists(3);
        node2.Color = RBTree<int, int>.Color.Red;
        node1.Color = RBTree<int, int>.Color.Black;
        node3.Color = RBTree<int, int>.Color.Black;

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Black, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(_tree.Root));
        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(node2));
        Assert.AreEqual("key: 1, color: Black, parent: 2, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 5, color: Black, parent: 2, left: 3, right: -", AsString(node5));
        Assert.AreEqual("key: 3, color: Red, parent: 5, left: -, right: -", AsString(node3));
    }

    [Test]
    public void Delete_Case_3_LeftChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        node1.Color = RBTree<int, int>.Color.Black;
        node10.Color = RBTree<int, int>.Color.Black;

        _tree.Delete(1);

        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: -", AsString(node1));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: -, right: 10", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: -, right: 10", AsString(node5));
        Assert.AreEqual("key: 10, color: Red, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_Case_3_RightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        node1.Color = RBTree<int, int>.Color.Black;
        node10.Color = RBTree<int, int>.Color.Black;

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Black, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: -", AsString(_tree.Root));
        Assert.AreEqual("key: 5, color: Black, parent: -, left: 1, right: -", AsString(node5));
        Assert.AreEqual("key: 1, color: Red, parent: 5, left: -, right: -", AsString(node1));
    }

    [Test]
    public void Delete_Case_5_LeftChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node11 = _tree.InsertOrGetExists(11);
        var node10 = _tree.InsertOrGetExists(10);

        _tree.Delete(1);

        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: -", AsString(node1));

        Assert.AreEqual("key: 10, color: Black, parent: -, left: 5, right: 11", AsString(_tree.Root));
        Assert.AreEqual("key: 10, color: Black, parent: -, left: 5, right: 11", AsString(node10));
        Assert.AreEqual("key: 5, color: Black, parent: 10, left: -, right: -", AsString(node5));
        Assert.AreEqual("key: 11, color: Black, parent: 10, left: -, right: -", AsString(node11));
    }

    [Test]
    public void Delete_Case_5_RightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node2 = _tree.InsertOrGetExists(2);

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Black, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(_tree.Root));
        Assert.AreEqual("key: 2, color: Black, parent: -, left: 1, right: 5", AsString(node2));
        Assert.AreEqual("key: 1, color: Black, parent: 2, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 5, color: Black, parent: 2, left: -, right: -", AsString(node5));
    }

    [Test]
    public void Delete_Case_6_LeftChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node11 = _tree.InsertOrGetExists(11);
        var node10 = _tree.InsertOrGetExists(10);
        var node12 = _tree.InsertOrGetExists(12);

        _tree.Delete(1);

        Assert.AreEqual("key: 1, color: Black, parent: -, left: -, right: -", AsString(node1));

        Assert.AreEqual("key: 11, color: Black, parent: -, left: 5, right: 12", AsString(_tree.Root));
        Assert.AreEqual("key: 11, color: Black, parent: -, left: 5, right: 12", AsString(node11));
        Assert.AreEqual("key: 5, color: Black, parent: 11, left: -, right: 10", AsString(node5));
        Assert.AreEqual("key: 12, color: Black, parent: 11, left: -, right: -", AsString(node12));
        Assert.AreEqual("key: 10, color: Red, parent: 5, left: -, right: -", AsString(node10));
    }

    [Test]
    public void Delete_Case_6_RightChild()
    {
        var node5 = _tree.InsertOrGetExists(5);
        var node1 = _tree.InsertOrGetExists(1);
        var node10 = _tree.InsertOrGetExists(10);
        var node2 = _tree.InsertOrGetExists(2);
        var node0 = _tree.InsertOrGetExists(0);

        _tree.Delete(10);

        Assert.AreEqual("key: 10, color: Black, parent: -, left: -, right: -", AsString(node10));

        Assert.AreEqual("key: 1, color: Black, parent: -, left: 0, right: 5", AsString(_tree.Root));
        Assert.AreEqual("key: 1, color: Black, parent: -, left: 0, right: 5", AsString(node1));
        Assert.AreEqual("key: 0, color: Black, parent: 1, left: -, right: -", AsString(node0));
        Assert.AreEqual("key: 5, color: Black, parent: 1, left: 2, right: -", AsString(node5));
        Assert.AreEqual("key: 2, color: Red, parent: 5, left: -, right: -", AsString(node2));
    }

    [Test]
    public void Delete_Hard_1()
    {
        var node13 = _tree.InsertOrGetExists(13);
        var node8 = _tree.InsertOrGetExists(8);
        var node17 = _tree.InsertOrGetExists(17);
        var node1 = _tree.InsertOrGetExists(1);
        var node11 = _tree.InsertOrGetExists(11);
        var node15 = _tree.InsertOrGetExists(15);
        var node25 = _tree.InsertOrGetExists(25);
        var node6 = _tree.InsertOrGetExists(6);
        var node22 = _tree.InsertOrGetExists(22);
        var node27 = _tree.InsertOrGetExists(27);

        _tree.Delete(8);

        Assert.AreEqual("key: 8, color: Red, parent: -, left: -, right: -", AsString(node8));

        Assert.AreEqual("key: 13, color: Black, parent: -, left: 6, right: 17", AsString(_tree.Root));
        Assert.AreEqual("key: 13, color: Black, parent: -, left: 6, right: 17", AsString(node13));
        Assert.AreEqual("key: 6, color: Red, parent: 13, left: 1, right: 11", AsString(node6));
        Assert.AreEqual("key: 17, color: Red, parent: 13, left: 15, right: 25", AsString(node17));
        Assert.AreEqual("key: 1, color: Black, parent: 6, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 11, color: Black, parent: 6, left: -, right: -", AsString(node11));
        Assert.AreEqual("key: 15, color: Black, parent: 17, left: -, right: -", AsString(node15));
        Assert.AreEqual("key: 25, color: Black, parent: 17, left: 22, right: 27", AsString(node25));
        Assert.AreEqual("key: 22, color: Red, parent: 25, left: -, right: -", AsString(node22));
        Assert.AreEqual("key: 27, color: Red, parent: 25, left: -, right: -", AsString(node27));
    }

    [Test]
    public void Delete_Hard_2()
    {
        var node7 = _tree.InsertOrGetExists(7);
        var node3 = _tree.InsertOrGetExists(3);
        var node18 = _tree.InsertOrGetExists(18);
        var node10 = _tree.InsertOrGetExists(10);
        var node22 = _tree.InsertOrGetExists(22);
        var node8 = _tree.InsertOrGetExists(8);
        var node11 = _tree.InsertOrGetExists(11);
        var node26 = _tree.InsertOrGetExists(26);

        _tree.Delete(3);

        Assert.AreEqual("key: 3, color: Black, parent: -, left: -, right: -", AsString(node3));

        Assert.AreEqual("key: 18, color: Black, parent: -, left: 10, right: 22", AsString(_tree.Root));
        Assert.AreEqual("key: 18, color: Black, parent: -, left: 10, right: 22", AsString(node18));
        Assert.AreEqual("key: 10, color: Red, parent: 18, left: 7, right: 11", AsString(node10));
        Assert.AreEqual("key: 22, color: Black, parent: 18, left: -, right: 26", AsString(node22));
        Assert.AreEqual("key: 7, color: Black, parent: 10, left: -, right: 8", AsString(node7));
        Assert.AreEqual("key: 11, color: Black, parent: 10, left: -, right: -", AsString(node11));
        Assert.AreEqual("key: 26, color: Red, parent: 22, left: -, right: -", AsString(node26));
        Assert.AreEqual("key: 8, color: Red, parent: 7, left: -, right: -", AsString(node8));
    }

    [Test]
    public void Delete_Hard_3()
    {
        var node13 = _tree.InsertOrGetExists(13);
        var node8 = _tree.InsertOrGetExists(8);
        var node17 = _tree.InsertOrGetExists(17);
        var node1 = _tree.InsertOrGetExists(1);
        var node11 = _tree.InsertOrGetExists(11);
        var node15 = _tree.InsertOrGetExists(15);
        var node25 = _tree.InsertOrGetExists(25);
        var node6 = _tree.InsertOrGetExists(6);
        var node22 = _tree.InsertOrGetExists(22);
        var node27 = _tree.InsertOrGetExists(27);

        _tree.Delete(11);

        Assert.AreEqual("key: 11, color: Black, parent: -, left: -, right: -", AsString(node11));

        Assert.AreEqual("key: 13, color: Black, parent: -, left: 6, right: 17", AsString(_tree.Root));
        Assert.AreEqual("key: 13, color: Black, parent: -, left: 6, right: 17", AsString(node13));
        Assert.AreEqual("key: 6, color: Red, parent: 13, left: 1, right: 8", AsString(node6));
        Assert.AreEqual("key: 17, color: Red, parent: 13, left: 15, right: 25", AsString(node17));
        Assert.AreEqual("key: 1, color: Black, parent: 6, left: -, right: -", AsString(node1));
        Assert.AreEqual("key: 8, color: Black, parent: 6, left: -, right: -", AsString(node8));
        Assert.AreEqual("key: 15, color: Black, parent: 17, left: -, right: -", AsString(node15));
        Assert.AreEqual("key: 25, color: Black, parent: 17, left: 22, right: 27", AsString(node25));
        Assert.AreEqual("key: 22, color: Red, parent: 25, left: -, right: -", AsString(node22));
        Assert.AreEqual("key: 27, color: Red, parent: 25, left: -, right: -", AsString(node27));
    }

    private string AsString(RBTree<int, int>.Node node)
    {
        return String.Format("key: {0}, color: {1}, parent: {2}, left: {3}, right: {4}",
            node.Key, node.Color, node.Parent != null ? node.Parent.Key.ToString() : "-", node.Left != null ? node.Left.Key.ToString() : "-", node.Right != null ? node.Right.Key.ToString() : "-");
    }
}
