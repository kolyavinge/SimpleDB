using System;
using System.Text;
using SimpleDB.Infrastructure;

namespace SimpleDB.DataStructures
{
    internal static class RBTreeSerializer
    {
        private static readonly byte _emptyTreeFlag = 255;

        public static void Serialize<TKey, TValue>(RBTree<TKey, TValue> tree, IWriteableStream stream) where TKey : IComparable<TKey>
        {
            if (tree.Root == null) stream.WriteByte(_emptyTreeFlag);
            else WriteNodeRec(tree.Root, stream);
        }

        private static void WriteNodeRec<TKey, TValue>(RBTree<TKey, TValue>.Node node, IWriteableStream stream) where TKey : IComparable<TKey>
        {
            WriteNode(node, stream);
            if (node.Left != null) WriteNodeRec(node.Left, stream);
            if (node.Right != null) WriteNodeRec(node.Right, stream);
        }

        private static void WriteNode<TKey, TValue>(RBTree<TKey, TValue>.Node node, IWriteableStream stream) where TKey : IComparable<TKey>
        {
            byte flags = node.Color == RBTree<TKey, TValue>.Color.Red ? (byte)0 : (byte)1;
            flags |= node.Left != null ? (byte)2 : (byte)0;
            flags |= node.Right != null ? (byte)4 : (byte)0;
            stream.WriteByte(flags);
            WriteNodeKey(node.Key, stream);
            var valueBytes = BinarySerialization.ToBinary(node.Value);
            stream.WriteInt(valueBytes.Length);
            stream.WriteByteArray(valueBytes, 0, valueBytes.Length);
        }

        private static void WriteNodeKey(object nodeKey, IWriteableStream stream)
        {
            var nodeKeyType = nodeKey.GetType();
            if (nodeKeyType == typeof(bool))
            {
                stream.WriteBool((bool)nodeKey);
            }
            else if (nodeKeyType == typeof(sbyte))
            {
                stream.WriteSByte((sbyte)nodeKey);
            }
            else if (nodeKeyType == typeof(byte))
            {
                stream.WriteByte((byte)nodeKey);
            }
            else if (nodeKeyType == typeof(char))
            {
                stream.WriteChar((char)nodeKey);
            }
            else if (nodeKeyType == typeof(short))
            {
                stream.WriteShort((short)nodeKey);
            }
            else if (nodeKeyType == typeof(ushort))
            {
                stream.WriteUShort((ushort)nodeKey);
            }
            else if (nodeKeyType == typeof(int))
            {
                stream.WriteInt((int)nodeKey);
            }
            else if (nodeKeyType == typeof(uint))
            {
                stream.WriteUInt((uint)nodeKey);
            }
            else if (nodeKeyType == typeof(long))
            {
                stream.WriteLong((long)nodeKey);
            }
            else if (nodeKeyType == typeof(ulong))
            {
                stream.WriteULong((ulong)nodeKey);
            }
            else if (nodeKeyType == typeof(float))
            {
                stream.WriteFloat((float)nodeKey);
            }
            else if (nodeKeyType == typeof(double))
            {
                stream.WriteDouble((double)nodeKey);
            }
            else if (nodeKeyType == typeof(decimal))
            {
                stream.WriteDecimal((decimal)nodeKey);
            }
            else if (nodeKeyType == typeof(DateTime))
            {
                stream.WriteLong(((DateTime)nodeKey).ToBinary());
            }
            else if (nodeKeyType == typeof(string))
            {
                var bytes = Encoding.UTF8.GetBytes((string)nodeKey);
                stream.WriteInt(bytes.Length);
                stream.WriteByteArray(bytes, 0, bytes.Length);
            }
            else
            {
                var fieldValueJson = JsonSerialization.ToJson(nodeKey);
                var bytes = Encoding.UTF8.GetBytes(fieldValueJson);
                stream.WriteInt(bytes.Length);
                stream.WriteByteArray(bytes, 0, bytes.Length);
            }
        }

        public static RBTree<TKey, TValue> Deserialize<TKey, TValue>(IReadableStream stream) where TKey : IComparable<TKey>
        {
            var emptyFlag = stream.ReadByte();
            if (emptyFlag == _emptyTreeFlag) return new RBTree<TKey, TValue>();
            else stream.Seek(-1, System.IO.SeekOrigin.Current);
            bool hasLeft, hasRight;
            var root = ReadNode<TKey, TValue>(stream, out hasLeft, out hasRight);
            if (hasLeft) ReadNodeRec(root, true, stream);
            if (hasRight) ReadNodeRec(root, false, stream);

            return new RBTree<TKey, TValue>(root);
        }

        private static void ReadNodeRec<TKey, TValue>(RBTree<TKey, TValue>.Node parent, bool toLeft, IReadableStream stream) where TKey : IComparable<TKey>
        {
            bool hasLeft, hasRight;
            var node = ReadNode<TKey, TValue>(stream, out hasLeft, out hasRight);
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

        private static RBTree<TKey, TValue>.Node ReadNode<TKey, TValue>(IReadableStream stream, out bool hasLeft, out bool hasRight) where TKey : IComparable<TKey>
        {
            var flags = stream.ReadByte();
            var color = (flags & 1) == 0 ? RBTree<TKey, TValue>.Color.Red : RBTree<TKey, TValue>.Color.Black;
            hasLeft = (flags & 2) > 0;
            hasRight = (flags & 4) > 0;
            var key = (TKey)ReadNodeKey<TKey>(stream);
            var valueLength = stream.ReadInt();
            var value = BinarySerialization.FromBinary<TValue>(stream.ReadByteArray(valueLength));

            return new RBTree<TKey, TValue>.Node(key) { Color = color, Value = value };
        }

        private static object ReadNodeKey<TKey>(IReadableStream stream)
        {
            var nodeKeyType = typeof(TKey);
            if (nodeKeyType == typeof(bool))
            {
                return stream.ReadBool();
            }
            else if (nodeKeyType == typeof(sbyte))
            {
                return stream.ReadSByte();
            }
            else if (nodeKeyType == typeof(byte))
            {
                return stream.ReadByte();
            }
            else if (nodeKeyType == typeof(char))
            {
                return stream.ReadChar();
            }
            else if (nodeKeyType == typeof(short))
            {
                return stream.ReadShort();
            }
            else if (nodeKeyType == typeof(ushort))
            {
                return stream.ReadUShort();
            }
            else if (nodeKeyType == typeof(int))
            {
                return stream.ReadInt();
            }
            else if (nodeKeyType == typeof(uint))
            {
                return stream.ReadUInt();
            }
            else if (nodeKeyType == typeof(long))
            {
                return stream.ReadLong();
            }
            else if (nodeKeyType == typeof(ulong))
            {
                return stream.ReadULong();
            }
            else if (nodeKeyType == typeof(float))
            {
                return stream.ReadFloat();
            }
            else if (nodeKeyType == typeof(double))
            {
                return stream.ReadDouble();
            }
            else if (nodeKeyType == typeof(decimal))
            {
                return stream.ReadDecimal();
            }
            else if (nodeKeyType == typeof(DateTime))
            {
                return DateTime.FromBinary(stream.ReadLong());
            }
            else if (nodeKeyType == typeof(string))
            {
                var length = stream.ReadInt();
                var bytes = stream.ReadByteArray(length);
                return Encoding.UTF8.GetString(bytes);
            }
            else
            {
                var length = stream.ReadInt();
                var bytes = stream.ReadByteArray(length);
                var fieldValueJson = Encoding.UTF8.GetString(bytes);
                return JsonSerialization.FromJson(typeof(TKey), fieldValueJson);
            }
        }
    }
}
