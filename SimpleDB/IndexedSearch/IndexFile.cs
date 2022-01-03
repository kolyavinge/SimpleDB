using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch
{
    internal class IndexFile
    {
        private readonly Type _primaryKeyType;
        private readonly IFileSystem _fileSystem;
        private readonly IDictionary<byte, Type> _fieldTypes;

        public string FileName { get; }

        public IndexFile(string fileName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection, IFileSystem fileSystem)
        {
            FileName = fileName;
            _primaryKeyType = primaryKeyType;
            _fileSystem = fileSystem;
            _fieldTypes = fieldMetaCollection.ToDictionary(k => k.Number, v => v.Type);
        }

        public Index<TField> ReadIndex<TField>() where TField : IComparable<TField>
        {
            using (var stream = _fileSystem.OpenFileRead(FileName))
            {
                return Index<TField>.Deserialize(stream, _primaryKeyType, _fieldTypes);
            }
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
        IndexFile Make(string entityName, string indexName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection);
    }

    internal class IndexFileFactory : IIndexFileFactory
    {
        private readonly IFileSystem _fileSystem;

        public IndexFileFactory(IFileSystem fileSystem)
        {
            _fileSystem = fileSystem;
        }

        public IndexFile Make(string entityName, string indexName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            var fileName = IndexFileName.FromEntityName(entityName, indexName);
            return new IndexFile(fileName, primaryKeyType, fieldMetaCollection, _fileSystem);
        }
    }

    internal static class IndexFileName
    {
        public const string Extension = ".index";

        public static string FromEntityName(string entityName, string indexName)
        {
            return String.Format("{0}_{1}{2}", entityName, indexName, Extension);
        }
    }
}
