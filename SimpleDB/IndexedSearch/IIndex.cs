using System;
using System.Collections.Generic;
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
