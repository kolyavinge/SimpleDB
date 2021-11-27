using System;
using System.Collections.Generic;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class MetaFile
    {
        private readonly string _fileFullPath;

        public MetaFile(string fileFullPath)
        {
            _fileFullPath = fileFullPath;
        }

        public Type GetPrimaryKeyType()
        {
            using (var fs = IOC.Get<IFileSystem>().OpenFileRead(_fileFullPath))
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
            using (var fs = IOC.Get<IFileSystem>().OpenFileRead(_fileFullPath))
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

        public void Save(Type primaryKeyType, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            using (var fs = IOC.Get<IFileSystem>().OpenFileWrite(_fileFullPath))
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
