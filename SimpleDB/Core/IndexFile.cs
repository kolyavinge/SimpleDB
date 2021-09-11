using System;
using System.Linq;
using System.Collections.Generic;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class IndexFile
    {
        private readonly string _fileFullPath;
        private readonly Type _primaryKeyType;
        private readonly IFileSystem _fileSystem;
        private readonly IDictionary<byte, Type> _fieldTypes;

        public IndexFile(string fileFullPath, Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            _fileFullPath = fileFullPath;
            _primaryKeyType = primaryKeyType;
            _fileSystem = IOC.Get<IFileSystem>();
            _fieldTypes = fieldMetaCollection.ToDictionary(k => k.Number, v => v.Type);
        }

        public Index<TField> ReadIndex<TField>() where TField : IComparable<TField>
        {
            using (var stream = _fileSystem.OpenFileRead(_fileFullPath))
            {
                return Index<TField>.Deserialize(stream, _primaryKeyType, _fieldTypes);
            }
        }

        public void WriteIndex<TField>(Index<TField> index) where TField : IComparable<TField>
        {
            using (var stream = _fileSystem.OpenFileWrite(_fileFullPath))
            {
                index.Serialize(stream);
            }
        }
    }

    internal static class IndexFileName
    {
        public static string Extension = ".index";

        public static string FromEntityName(string entityName, string indexName)
        {
            return String.Format("{0}_{1}{2}", entityName, indexName, Extension);
        }
    }
}
