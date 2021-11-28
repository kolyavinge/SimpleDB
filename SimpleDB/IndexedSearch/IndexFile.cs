using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch
{
    internal class IndexFile
    {
        private readonly string _fileFullPath;
        private readonly Type _primaryKeyType;
        private readonly IFileSystem _fileSystem;
        private readonly IDictionary<byte, Type> _fieldTypes;

        public IndexFile(string fileFullPath, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection, IFileSystem fileSystem)
        {
            _fileFullPath = fileFullPath;
            _primaryKeyType = primaryKeyType;
            _fileSystem = fileSystem;
            _fieldTypes = fieldMetaCollection.ToDictionary(k => k.Number, v => v.Type);
        }

        public Index<TField> ReadIndex<TField>() where TField : IComparable<TField>
        {
            using (var stream = _fileSystem.OpenFileRead(_fileFullPath))
            {
                return Index<TField>.Deserialize(stream, _primaryKeyType, _fieldTypes);
            }
        }

        public void WriteIndex(IIndex index)
        {
            using (var stream = _fileSystem.OpenFileWrite(_fileFullPath))
            {
                index.Serialize(stream);
                stream.SetLength(stream.Position);
            }
        }
    }

    internal interface IIndexFileFactory
    {
        IndexFile Make(string entityName, string indexName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection);
    }

    internal class IndexFileFactory : IIndexFileFactory
    {
        private readonly string _workingDirectory;
        private readonly IFileSystem _fileSystem;

        public IndexFileFactory(string workingDirectory, IFileSystem fileSystem = null)
        {
            _workingDirectory = workingDirectory;
            _fileSystem = fileSystem ?? FileSystem.Instance;
        }

        public IndexFile Make(string entityName, string indexName, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            var fileFullPath = IndexFileName.GetFullFileName(_workingDirectory, entityName, indexName);
            return new IndexFile(fileFullPath, primaryKeyType, fieldMetaCollection, _fileSystem);
        }
    }

    internal static class IndexFileName
    {
        public static string Extension = ".index";

        public static string GetFullFileName(string workingDirectory, string entityName, string indexName)
        {
            return String.Format("{0}\\{1}_{2}{3}", workingDirectory, entityName, indexName, Extension);
        }
    }
}
