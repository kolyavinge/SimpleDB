using System;
using SimpleDB.Infrastructure;

namespace SimpleDB.DataStructures;

internal interface IRBTreeNodeSerializer<TKey, TValue> where TKey : IComparable<TKey>
{
    void SerializeKey(TKey nodeKey, IWriteableStream stream);
    void SerializeValue(TValue nodeValue, IWriteableStream stream);
}

internal interface IRBTreeNodeDeserializer<TKey, TValue> where TKey : IComparable<TKey>
{
    TKey DeserializeKey(IReadableStream stream);
    TValue DeserializeValue(IReadableStream stream);
}

internal class RBTreeSerializer<TKey, TValue> where TKey : IComparable<TKey>
{
    private const byte _emptyTreeFlag = 255;
    private readonly IRBTreeNodeSerializer<TKey, TValue> _nodeSerializer;

    public RBTreeSerializer(IRBTreeNodeSerializer<TKey, TValue> nodeSerializer)
    {
        _nodeSerializer = nodeSerializer;
    }

    public void Serialize(RBTree<TKey, TValue> tree, IWriteableStream stream)
    {
        if (tree.Root is null) stream.WriteByte(_emptyTreeFlag);
        else WriteNodeRec(tree.Root, stream);
    }

    private void WriteNodeRec(RBTree<TKey, TValue>.Node node, IWriteableStream stream)
    {
        WriteNode(node, stream);
        if (node.Left is not null) WriteNodeRec(node.Left, stream);
        if (node.Right is not null) WriteNodeRec(node.Right, stream);
    }

    private void WriteNode(RBTree<TKey, TValue>.Node node, IWriteableStream stream)
    {
        byte flags = node.Color == RBTree<TKey, TValue>.Color.Red ? (byte)0 : (byte)1;
        flags |= node.Left is not null ? (byte)2 : (byte)0;
        flags |= node.Right is not null ? (byte)4 : (byte)0;
        stream.WriteByte(flags);
        _nodeSerializer.SerializeKey(node.Key, stream);
        _nodeSerializer.SerializeValue(node.Value, stream);
    }
}

internal class RBTreeDeserializer<TKey, TValue> where TKey : IComparable<TKey>
{
    private const byte _emptyTreeFlag = 255;

    private readonly IRBTreeNodeDeserializer<TKey, TValue> _nodeSerializer;

    public RBTreeDeserializer(IRBTreeNodeDeserializer<TKey, TValue> nodeSerializer)
    {
        _nodeSerializer = nodeSerializer;
    }

    public RBTree<TKey, TValue> Deserialize(IReadableStream stream)
    {
        var emptyFlag = stream.ReadByte();
        if (emptyFlag == _emptyTreeFlag) return new RBTree<TKey, TValue>();
        else stream.Seek(-1, System.IO.SeekOrigin.Current);
        var root = ReadNode(stream, out bool hasLeft, out bool hasRight);
        if (hasLeft) ReadNodeRec(root, true, stream);
        if (hasRight) ReadNodeRec(root, false, stream);

        return new RBTree<TKey, TValue>(root);
    }

    private void ReadNodeRec(RBTree<TKey, TValue>.Node parent, bool toLeft, IReadableStream stream)
    {
        var node = ReadNode(stream, out bool hasLeft, out bool hasRight);
        if (toLeft)
        {
            parent.Left = node;
            node.Parent = parent;
        }
        else
        {
            parent.Right = node;
            node.Parent = parent;
        }
        if (hasLeft) ReadNodeRec(node, true, stream);
        if (hasRight) ReadNodeRec(node, false, stream);
    }

    private RBTree<TKey, TValue>.Node ReadNode(IReadableStream stream, out bool hasLeft, out bool hasRight)
    {
        var flags = stream.ReadByte();
        var color = (flags & 1) == 0 ? RBTree<TKey, TValue>.Color.Red : RBTree<TKey, TValue>.Color.Black;
        hasLeft = (flags & 2) > 0;
        hasRight = (flags & 4) > 0;
        var key = _nodeSerializer.DeserializeKey(stream);
        var value = _nodeSerializer.DeserializeValue(stream);

        return new RBTree<TKey, TValue>.Node(key, value) { Color = color };
    }
}
