using System;
using System.Collections.Generic;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class MetaFile
    {
        private readonly IFileSystem _fileSystem;

        public string FileFullPath { get; }

        public MetaFile(string fileFullPath, IFileSystem fileSystem)
        {
            FileFullPath = fileFullPath;
            _fileSystem = fileSystem;
        }

        public Type GetPrimaryKeyType()
        {
            using (var fs = _fileSystem.OpenFileRead(FileFullPath))
            {
                return ReadPrimaryKeyType(fs);
            }
        }

        private Type ReadPrimaryKeyType(IReadableStream stream)
        {
            var fieldType = (FieldTypes)stream.ReadByte();
            var type = fieldType == FieldTypes.Object ? Type.GetType(stream.ReadString()) : FieldTypesConverter.GetType(fieldType);
            return type;
        }

        public IEnumerable<FieldMeta> GetFieldMetaCollection()
        {
            using (var fs = _fileSystem.OpenFileRead(FileFullPath))
            {
                var length = fs.Length;
                ReadPrimaryKeyType(fs); // skip
                while (fs.Position < length)
                {
                    var number = fs.ReadByte();
                    var fieldType = (FieldTypes)fs.ReadByte();
                    var type = fieldType == FieldTypes.Object ? Type.GetType(fs.ReadString()) : FieldTypesConverter.GetType(fieldType);
                    var compressed = fs.ReadBool();
                    yield return new FieldMeta(number, type) { Settings = new FieldSettings { Compressed = compressed } };
                }
            }
        }

        public bool IsExist()
        {
            return _fileSystem.FileExists(FileFullPath);
        }

        public void Save(Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            using (var fs = _fileSystem.OpenFileWrite(FileFullPath))
            {
                // save primary key
                var primaryKeyFieldType = FieldTypesConverter.GetFieldType(primaryKeyType);
                fs.WriteByte((byte)primaryKeyFieldType);
                if (primaryKeyFieldType == FieldTypes.Object)
                {
                    fs.WriteString(primaryKeyType.AssemblyQualifiedName);
                }
                // save fields
                foreach (var fieldMeta in fieldMetaCollection)
                {
                    fs.WriteByte(fieldMeta.Number);
                    var fieldType = FieldTypesConverter.GetFieldType(fieldMeta.Type);
                    fs.WriteByte((byte)fieldType);
                    if (fieldType == FieldTypes.Object)
                    {
                        fs.WriteString(fieldMeta.Type.AssemblyQualifiedName);
                    }
                    fs.WriteBool(fieldMeta.Settings.Compressed);
                }
            }
        }

        public void Delete()
        {
            _fileSystem.DeleteFile(FileFullPath);
        }
    }

    internal interface IMetaFileFactory
    {
        MetaFile MakeFromFileFullPath(string fileFullPath);
        MetaFile MakeFromEntityName(string entityName);
    }

    internal class MetaFileFactory : IMetaFileFactory
    {
        private readonly string _workingDirectory;
        private readonly IFileSystem _fileSystem;

        public MetaFileFactory(string workingDirectory, IFileSystem fileSystem = null)
        {
            _workingDirectory = workingDirectory;
            _fileSystem = fileSystem ?? FileSystem.Instance;
        }

        public MetaFile MakeFromFileFullPath(string fileFullPath)
        {
            return new MetaFile(fileFullPath, _fileSystem);
        }

        public MetaFile MakeFromEntityName(string entityName)
        {
            return MakeFromFileFullPath(MetaFileName.GetFullFileName(_workingDirectory, entityName));
        }
    }

    internal static class MetaFileName
    {
        public static string Extension = ".meta";

        public static string GetFullFileName(string workingDirectory, string entityName)
        {
            return String.Format("{0}\\{1}{2}", workingDirectory, entityName, Extension);
        }
    }
}
