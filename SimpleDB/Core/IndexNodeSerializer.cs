using System;
using System.Collections.Generic;
using System.Text;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class IndexNodeSerializer<TField> : IRBTreeNodeSerializer<TField, IndexValue> where TField : IComparable<TField>
    {
        private readonly IndexMeta _indexMeta;
        private readonly Type _primaryKeyType;
        private readonly IDictionary<byte, Type> _fieldTypes;
        private TField _lastKey;

        public IndexNodeSerializer() { }

        public IndexNodeSerializer(IndexMeta indexMeta, Type primaryKeyType, IDictionary<byte, Type> fieldTypes)
        {
            _indexMeta = indexMeta;
            _primaryKeyType = primaryKeyType;
            _fieldTypes = fieldTypes;
        }

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
                if (item.IncludedFields != null)
                {
                    foreach (var includedField in item.IncludedFields)
                    {
                        SerializeObject(includedField, stream);
                    }
                }
            }
        }

        public TField DeserializeKey(IReadableStream stream)
        {
            return _lastKey = (TField)DeserializeObject(typeof(TField), stream);
        }

        public IndexValue DeserializeValue(IReadableStream stream)
        {
            var indexValue = new IndexValue();
            indexValue.IndexedFieldValue = _lastKey;
            var itemsCount = stream.ReadInt();
            indexValue.Items = new List<IndexItem>(itemsCount);
            for (int itemIndex = 0; itemIndex < itemsCount; itemIndex++)
            {
                var indexItem = new IndexItem();
                indexItem.PrimaryKeyValue = DeserializeObject(_primaryKeyType, stream);
                indexItem.IncludedFields = new object[_indexMeta.IncludedFieldNumbers.Length];
                for (int includedFieldIndex = 0; includedFieldIndex < _indexMeta.IncludedFieldNumbers.Length; includedFieldIndex++)
                {
                    var includedFieldNumber = _indexMeta.IncludedFieldNumbers[includedFieldIndex];
                    var includedFieldType = _fieldTypes[includedFieldNumber];
                    indexItem.IncludedFields[includedFieldIndex] = DeserializeObject(includedFieldType, stream);
                }
                indexValue.Items.Add(indexItem);
            }

            return indexValue;
        }

        private static void SerializeObject(object obj, IWriteableStream stream)
        {
            if (obj.GetType() == typeof(bool))
            {
                stream.WriteBool((bool)obj);
            }
            else if (obj.GetType() == typeof(sbyte))
            {
                stream.WriteSByte((sbyte)obj);
            }
            else if (obj.GetType() == typeof(byte))
            {
                stream.WriteByte((byte)obj);
            }
            else if (obj.GetType() == typeof(char))
            {
                stream.WriteChar((char)obj);
            }
            else if (obj.GetType() == typeof(short))
            {
                stream.WriteShort((short)obj);
            }
            else if (obj.GetType() == typeof(ushort))
            {
                stream.WriteUShort((ushort)obj);
            }
            else if (obj.GetType() == typeof(int))
            {
                stream.WriteInt((int)obj);
            }
            else if (obj.GetType() == typeof(uint))
            {
                stream.WriteUInt((uint)obj);
            }
            else if (obj.GetType() == typeof(long))
            {
                stream.WriteLong((long)obj);
            }
            else if (obj.GetType() == typeof(ulong))
            {
                stream.WriteULong((ulong)obj);
            }
            else if (obj.GetType() == typeof(float))
            {
                stream.WriteFloat((float)obj);
            }
            else if (obj.GetType() == typeof(double))
            {
                stream.WriteDouble((double)obj);
            }
            else if (obj.GetType() == typeof(decimal))
            {
                stream.WriteDecimal((decimal)obj);
            }
            else if (obj.GetType() == typeof(DateTime))
            {
                stream.WriteLong(((DateTime)obj).ToBinary());
            }
            else if (obj.GetType() == typeof(string))
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

        private static object DeserializeObject(Type type, IReadableStream stream)
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
}
