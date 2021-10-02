using System;
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

    internal abstract class AbstractIndex
    {
        public IndexMeta Meta { get; private set; }

        public AbstractIndex(IndexMeta meta)
        {
            Meta = meta;
        }

        public abstract void Add(object indexedFieldValue, IndexItem indexItem);

        public abstract void Serialize(IWriteableStream stream);
    }
}
