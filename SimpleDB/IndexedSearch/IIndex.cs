﻿using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch
{
    internal class IndexMeta
    {
        public Type EntityType { get; set; }

        public Type IndexedFieldType { get; set; }

        public string Name { get; set; }

        public byte IndexedFieldNumber { get; set; }

        public byte[] IncludedFieldNumbers { get; set; }

        public byte[] GetAllFieldNumbers()
        {
            byte[] result;
            if (IncludedFieldNumbers != null)
            {
                result = new byte[IncludedFieldNumbers.Length + 1];
                Array.Copy(IncludedFieldNumbers, 0, result, 1, IncludedFieldNumbers.Length);
                result[0] = IndexedFieldNumber;
            }
            else
            {
                result = new byte[] { IndexedFieldNumber };
            }

            return result;
        }

        public bool IsContainAnyFields(ISet<byte> fieldNumberSet)
        {
            if (fieldNumberSet.Contains(IndexedFieldNumber)) return true;
            if (IncludedFieldNumbers != null)
            {
                return IncludedFieldNumbers.Any(fieldNumberSet.Contains);
            }

            return false;
        }

        public static IndexMeta Deserialize(IReadableStream stream)
        {
            var meta = new IndexMeta();
            meta.EntityType = Type.GetType(stream.ReadString());
            meta.IndexedFieldType = Type.GetType(stream.ReadString());
            meta.Name = stream.ReadString();
            meta.IndexedFieldNumber = stream.ReadByte();
            var includedFieldNumbersLength = stream.ReadByte();
            meta.IncludedFieldNumbers = stream.ReadByteArray(includedFieldNumbersLength);

            return meta;
        }

        public void Serialize(IWriteableStream stream)
        {
            stream.WriteString(EntityType.AssemblyQualifiedName);
            stream.WriteString(IndexedFieldType.AssemblyQualifiedName);
            stream.WriteString(Name);
            stream.WriteByte(IndexedFieldNumber);
            stream.WriteByte((byte)IncludedFieldNumbers.Length);
            stream.WriteByteArray(IncludedFieldNumbers, 0, IncludedFieldNumbers.Length);
        }
    }

    internal interface IIndex
    {
        IndexMeta Meta { get; }

        IndexValue GetEquals(object fieldValue);

        IEnumerable<IndexValue> GetNotEquals(object fieldValue);

        IEnumerable<IndexValue> GetLess(object fieldValue);

        IEnumerable<IndexValue> GetGreat(object fieldValue);

        IEnumerable<IndexValue> GetLessOrEquals(object fieldValue);

        IEnumerable<IndexValue> GetGreatOrEquals(object fieldValue);

        IEnumerable<IndexValue> GetLike(object fieldValue);

        IEnumerable<IndexValue> GetNotLike(object fieldValue);

        IEnumerable<IndexValue> GetIn(IEnumerable<object> fieldValues);

        IEnumerable<IndexValue> GetNotIn(IEnumerable<object> fieldValues);

        void Add(object indexedFieldValue, IndexItem indexItem);

        void Add(object indexedFieldValue, IEnumerable<IndexItem> indexItems);

        void Delete(object indexedFieldValue);

        IEnumerable<IndexValue> GetAllIndexValues();

        void Serialize(IWriteableStream stream);
    }
}
