﻿using System;
using System.Collections.Generic;
using System.Text;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch;

internal class IndexNodeSerializer<TField> : IRBTreeNodeSerializer<TField, IndexValue> where TField : IComparable<TField>
{
    public void SerializeKey(TField nodeKey, IWriteableStream stream)
    {
        SerializeObject(nodeKey, stream);
    }

    public void SerializeValue(IndexValue nodeValue, IWriteableStream stream)
    {
        // св-во IndexedFieldValue не сериализуется
        stream.WriteInt(nodeValue.Items.Count);
        foreach (var item in nodeValue.Items)
        {
            SerializeObject(item.PrimaryKeyValue, stream);
            if (item.IncludedFields is not null)
            {
                foreach (var includedField in item.IncludedFields)
                {
                    SerializeObject(includedField, stream);
                }
            }
        }
    }

    private static void SerializeObject(object obj, IWriteableStream stream)
    {
        if (obj is bool)
        {
            stream.WriteBool((bool)obj);
        }
        else if (obj is sbyte)
        {
            stream.WriteSByte((sbyte)obj);
        }
        else if (obj is byte)
        {
            stream.WriteByte((byte)obj);
        }
        else if (obj is char)
        {
            stream.WriteChar((char)obj);
        }
        else if (obj is short)
        {
            stream.WriteShort((short)obj);
        }
        else if (obj is ushort)
        {
            stream.WriteUShort((ushort)obj);
        }
        else if (obj is int)
        {
            stream.WriteInt((int)obj);
        }
        else if (obj is uint)
        {
            stream.WriteUInt((uint)obj);
        }
        else if (obj is long)
        {
            stream.WriteLong((long)obj);
        }
        else if (obj is ulong)
        {
            stream.WriteULong((ulong)obj);
        }
        else if (obj is float)
        {
            stream.WriteFloat((float)obj);
        }
        else if (obj is double)
        {
            stream.WriteDouble((double)obj);
        }
        else if (obj is decimal)
        {
            stream.WriteDecimal((decimal)obj);
        }
        else if (obj is DateTime)
        {
            stream.WriteLong(((DateTime)obj).ToBinary());
        }
        else if (obj is string)
        {
            var bytes = Encoding.UTF8.GetBytes((string)obj);
            stream.WriteInt(bytes.Length);
            stream.WriteByteArray(bytes, 0, bytes.Length);
        }
        else
        {
            var json = JsonSerialization.ToJson(obj);
            var bytes = Encoding.UTF8.GetBytes(json);
            stream.WriteInt(bytes.Length);
            stream.WriteByteArray(bytes, 0, bytes.Length);
        }
    }
}

internal class IndexNodeDeserializer<TField> : IRBTreeNodeDeserializer<TField, IndexValue> where TField : IComparable<TField>
{
    private readonly IndexMeta _indexMeta;
    private readonly Type _primaryKeyType;
    private readonly IDictionary<byte, Type> _fieldTypes;
    private TField? _lastKey;

    public IndexNodeDeserializer(IndexMeta indexMeta, Type primaryKeyType, IDictionary<byte, Type> fieldTypes)
    {
        _indexMeta = indexMeta;
        _primaryKeyType = primaryKeyType;
        _fieldTypes = fieldTypes;
    }

    public TField DeserializeKey(IReadableStream stream)
    {
        return _lastKey = (TField)DeserializeObject(typeof(TField), stream)!;
    }

    public IndexValue DeserializeValue(IReadableStream stream)
    {
        var itemsCount = stream.ReadInt();
        var indexValue = new IndexValue(_lastKey!, new List<IndexItem>(itemsCount));
        for (int itemIndex = 0; itemIndex < itemsCount; itemIndex++)
        {
            var primaryKeyValue = DeserializeObject(_primaryKeyType, stream)!;
            var includedFieldNumbers = _indexMeta.IncludedFieldNumbers is not null
                ? new object[_indexMeta.IncludedFieldNumbers.Length]
                : null;
            var indexItem = new IndexItem(primaryKeyValue, includedFieldNumbers);
            if (_indexMeta.IncludedFieldNumbers is not null)
            {
                for (int includedFieldIndex = 0; includedFieldIndex < _indexMeta.IncludedFieldNumbers.Length; includedFieldIndex++)
                {
                    var includedFieldNumber = _indexMeta.IncludedFieldNumbers[includedFieldIndex];
                    var includedFieldType = _fieldTypes[includedFieldNumber];
                    indexItem.IncludedFields![includedFieldIndex] = DeserializeObject(includedFieldType, stream);
                }
                indexValue.Items.Add(indexItem);
            }
        }

        return indexValue;
    }

    private static object? DeserializeObject(Type type, IReadableStream stream)
    {
        if (type == typeof(bool))
        {
            return stream.ReadBool();
        }
        else if (type == typeof(sbyte))
        {
            return stream.ReadSByte();
        }
        else if (type == typeof(byte))
        {
            return stream.ReadByte();
        }
        else if (type == typeof(char))
        {
            return stream.ReadChar();
        }
        else if (type == typeof(short))
        {
            return stream.ReadShort();
        }
        else if (type == typeof(ushort))
        {
            return stream.ReadUShort();
        }
        else if (type == typeof(int))
        {
            return stream.ReadInt();
        }
        else if (type == typeof(uint))
        {
            return stream.ReadUInt();
        }
        else if (type == typeof(long))
        {
            return stream.ReadLong();
        }
        else if (type == typeof(ulong))
        {
            return stream.ReadULong();
        }
        else if (type == typeof(float))
        {
            return stream.ReadFloat();
        }
        else if (type == typeof(double))
        {
            return stream.ReadDouble();
        }
        else if (type == typeof(decimal))
        {
            return stream.ReadDecimal();
        }
        else if (type == typeof(DateTime))
        {
            return DateTime.FromBinary(stream.ReadLong());
        }
        else if (type == typeof(string))
        {
            var length = stream.ReadInt();
            var bytes = stream.ReadByteArray(length);
            return Encoding.UTF8.GetString(bytes);
        }
        else
        {
            var length = stream.ReadInt();
            var bytes = stream.ReadByteArray(length);
            var json = Encoding.UTF8.GetString(bytes);
            return JsonSerialization.FromJson(type, json);
        }
    }
}
