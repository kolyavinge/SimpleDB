using System;
using System.Collections.Generic;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch;

internal static class IndexDeserializer
{
    public static Index<TField> Deserialize<TField>(IReadableStream stream, Type primaryKeyType, IDictionary<byte, Type> fieldTypes) where TField : IComparable<TField>
    {
        var indexMeta = IndexMeta.Deserialize(stream);
        return Deserialize<TField>(stream, indexMeta, primaryKeyType, fieldTypes);
    }

    public static IIndex Deserialize(IReadableStream stream, Type primaryKeyType, IDictionary<byte, Type> fieldTypes)
    {
        var indexMeta = IndexMeta.Deserialize(stream);
        var fieldType = indexMeta.IndexedFieldType;
        if (fieldType == null) throw new ArgumentException($"IndexedFieldType cannot be null");
        if (fieldType == typeof(bool)) return Deserialize<bool>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(sbyte)) return Deserialize<sbyte>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(byte)) return Deserialize<byte>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(char)) return Deserialize<char>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(short)) return Deserialize<short>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(ushort)) return Deserialize<ushort>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(int)) return Deserialize<int>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(uint)) return Deserialize<uint>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(long)) return Deserialize<long>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(ulong)) return Deserialize<ulong>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(float)) return Deserialize<float>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(double)) return Deserialize<double>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(decimal)) return Deserialize<decimal>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(DateTime)) return Deserialize<DateTime>(stream, indexMeta, primaryKeyType, fieldTypes);
        if (fieldType == typeof(string)) return Deserialize<string>(stream, indexMeta, primaryKeyType, fieldTypes);
        throw new ArgumentException($"Cannot read index for type '{fieldType}'");
    }

    private static Index<TField> Deserialize<TField>(IReadableStream stream, IndexMeta indexMeta, Type primaryKeyType, IDictionary<byte, Type> fieldTypes) where TField : IComparable<TField>
    {
        var deserializer = new RBTreeDeserializer<TField, IndexValue>(new IndexNodeDeserializer<TField>(indexMeta, primaryKeyType, fieldTypes));
        var indexTree = deserializer.Deserialize(stream);

        return new Index<TField>(indexMeta, indexTree);
    }
}
