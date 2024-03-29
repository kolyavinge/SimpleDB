﻿using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch;

internal class IndexFile
{
    private readonly Type _primaryKeyType;
    private readonly IFileSystem _fileSystem;
    private readonly IMemory _memory;
    private readonly IDictionary<byte, Type> _fieldTypes;

    public string FileName { get; }

    public IndexFile(string fileName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection, IFileSystem fileSystem, IMemory memory)
    {
        FileName = fileName;
        _primaryKeyType = primaryKeyType;
        _fileSystem = fileSystem;
        _memory = memory;
        _fieldTypes = fieldMetaCollection.ToDictionary(k => k.Number, v => v.Type);
    }

    public Index<TField> ReadIndex<TField>() where TField : IComparable<TField>
    {
        return IndexDeserializer.Deserialize<TField>(ReadIndexFileContent(), _primaryKeyType, _fieldTypes);
    }

    public IIndex ReadIndex()
    {
        return IndexDeserializer.Deserialize(ReadIndexFileContent(), _primaryKeyType, _fieldTypes);
    }

    private IMemoryBuffer ReadIndexFileContent()
    {
        byte[] indexFileBytes;
        using (var stream = _fileSystem.OpenFileRead(FileName))
        {
            indexFileBytes = stream.ReadByteArray((int)stream.Length);
        }
        var buffer = _memory.GetBuffer();
        buffer.WriteByteArray(indexFileBytes, 0, indexFileBytes.Length);
        buffer.Seek(0, System.IO.SeekOrigin.Begin);

        return buffer;
    }

    public void WriteIndex(IIndex index)
    {
        using (var stream = _fileSystem.OpenFileReadWrite(FileName))
        {
            index.Serialize(stream);
            stream.SetLength(stream.Position);
        }
    }

    public void Create()
    {
        _fileSystem.CreateFiles(FileName);
    }

    public bool IsExist()
    {
        return _fileSystem.FileExists(FileName);
    }
}

internal interface IIndexFileFactory
{
    IndexFile Make(string fileName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection);
    IndexFile Make(string entityName, string indexName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection);
}

internal class IndexFileFactory : IIndexFileFactory
{
    private readonly IFileSystem _fileSystem;
    private readonly IMemory _memory;

    public IndexFileFactory(IFileSystem fileSystem, IMemory? memory = null)
    {
        _fileSystem = fileSystem;
        _memory = memory ?? Memory.Instance;
    }

    public IndexFile Make(string fileName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection)
    {
        return new IndexFile(fileName, primaryKeyType, fieldMetaCollection, _fileSystem, _memory);
    }

    public IndexFile Make(string entityName, string indexName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection)
    {
        var fileName = IndexFileName.FromEntityName(entityName, indexName);
        return new IndexFile(fileName, primaryKeyType, fieldMetaCollection, _fileSystem, _memory);
    }
}

internal static class IndexFileName
{
    public const string Extension = ".idx";
    public const string Separator = "#";

    public static string FromEntityName(string entityName, string indexName)
    {
        return $"{entityName}{Separator}{indexName}{Extension}";
    }

    public static (string, string) GetEntityAndIndexName(string indexFileName)
    {
        var x = indexFileName.Split(new[] { Separator, Extension }, StringSplitOptions.None);
        return (x[0], x[1]);
    }
}
