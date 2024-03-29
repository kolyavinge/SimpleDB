﻿using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch;

internal class IndexMeta
{
    public string EntityName { get; }

    public Type IndexedFieldType { get; }

    public string Name { get; }

    public byte IndexedFieldNumber { get; }

    public byte[]? IncludedFieldNumbers { get; set; }

    public IndexMeta(string entityName, Type indexedFieldType, string name, byte indexedFieldNumber, byte[]? includedFieldNumbers = null)
    {
        EntityName = entityName;
        IndexedFieldType = indexedFieldType;
        Name = name;
        IndexedFieldNumber = indexedFieldNumber;
        IncludedFieldNumbers = includedFieldNumbers;
    }

    public byte[] GetAllFieldNumbers()
    {
        byte[] result;
        if (IncludedFieldNumbers is not null)
        {
            result = new byte[IncludedFieldNumbers.Length + 1];
            Array.Copy(IncludedFieldNumbers, 0, result, 1, IncludedFieldNumbers.Length);
            result[0] = IndexedFieldNumber;
        }
        else
        {
            result = new[] { IndexedFieldNumber };
        }

        return result;
    }

    public bool IsContainAnyFields(ISet<byte> fieldNumberSet)
    {
        if (fieldNumberSet.Contains(IndexedFieldNumber)) return true;
        if (IncludedFieldNumbers is not null)
        {
            return IncludedFieldNumbers.Any(fieldNumberSet.Contains);
        }

        return false;
    }

    public static IndexMeta Deserialize(IReadableStream stream)
    {
        var entityName = stream.ReadString();
        var indexedFieldType = Type.GetType(stream.ReadString());
        var name = stream.ReadString();
        var indexedFieldNumber = stream.ReadByte();
        var includedFieldNumbersLength = stream.ReadByte();
        var includedFieldNumbers = stream.ReadByteArray(includedFieldNumbersLength);

        return new IndexMeta(entityName, indexedFieldType, name, indexedFieldNumber, includedFieldNumbers);
    }

    public void Serialize(IWriteableStream stream)
    {
        stream.WriteString(EntityName);
        stream.WriteString(IndexedFieldType.AssemblyQualifiedName);
        stream.WriteString(Name);
        stream.WriteByte(IndexedFieldNumber);
        if (IncludedFieldNumbers is not null)
        {
            stream.WriteByte((byte)IncludedFieldNumbers.Length);
            stream.WriteByteArray(IncludedFieldNumbers, 0, IncludedFieldNumbers.Length);
        }
        else
        {
            stream.WriteByte(0);
        }
    }
}

internal class IndexValue
{
    public object IndexedFieldValue { get; }

    public List<IndexItem> Items { get; }

    public IndexValue(object indexedFieldValue, List<IndexItem> items)
    {
        IndexedFieldValue = indexedFieldValue;
        Items = items;
    }
}

internal class IndexItem
{
    public object PrimaryKeyValue { get; }

    public object?[]? IncludedFields { get; }

    public IndexItem(object primaryKeyValue, object?[]? includedFields)
    {
        PrimaryKeyValue = primaryKeyValue;
        IncludedFields = includedFields;
    }
}

internal interface IIndex
{
    IndexMeta Meta { get; }

    IndexValue? GetEquals(object fieldValue);

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

    IEnumerable<IndexValue> GetAllIndexValues(SortDirection sortDirection = SortDirection.Asc);

    void Serialize(IWriteableStream stream);
}
