using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;
using SimpleDB.Utils.EnumerableExtension;

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

        public MetaData GetMetaData()
        {
            using (var fs = _fileSystem.OpenFileRead(FileFullPath))
            {
                var entityTypeName = fs.ReadString();

                var primaryKeyFieldType = (FieldTypes)fs.ReadByte();
                var primaryKeyType = primaryKeyFieldType == FieldTypes.Object ? Type.GetType(fs.ReadString()) : FieldTypesConverter.GetType(primaryKeyFieldType);

                var primaryKeyName = fs.ReadString();

                var fieldMetaCollection = new List<FieldMeta>();
                int fieldMetaCollectionCount = fs.ReadInt();
                for (int i = 0; i < fieldMetaCollectionCount; i++)
                {
                    var number = fs.ReadByte();
                    var fieldType = (FieldTypes)fs.ReadByte();
                    var type = fieldType == FieldTypes.Object ? Type.GetType(fs.ReadString()) : FieldTypesConverter.GetType(fieldType);
                    var compressed = fs.ReadBool();
                    fieldMetaCollection.Add(new FieldMeta(number, type) { Settings = new FieldSettings { Compressed = compressed } });
                }

                var fieldNameCollection = new List<FieldName>();
                var fieldNameCollectionCount = fs.ReadInt();
                for (int i = 0; i < fieldNameCollectionCount; i++)
                {
                    var number = fs.ReadByte();
                    var name = fs.ReadString();
                    fieldNameCollection.Add(new FieldName(number, name));
                }

                return new MetaData
                {
                    EntityTypeName = entityTypeName,
                    PrimaryKeyType = primaryKeyType,
                    PrimaryKeyName = primaryKeyName,
                    FieldMetaCollection = fieldMetaCollection,
                    FieldNameCollection = fieldNameCollection
                };
            }
        }

        public void Save(MetaData metaData)
        {
            using (var fs = _fileSystem.OpenFileWrite(FileFullPath))
            {
                fs.WriteString(metaData.EntityTypeName);

                var primaryKeyFieldType = FieldTypesConverter.GetFieldType(metaData.PrimaryKeyType);
                fs.WriteByte((byte)primaryKeyFieldType);
                if (primaryKeyFieldType == FieldTypes.Object)
                {
                    fs.WriteString(metaData.PrimaryKeyType.AssemblyQualifiedName);
                }

                fs.WriteString(metaData.PrimaryKeyName);

                fs.WriteInt(metaData.FieldMetaCollection.Count());
                foreach (var fieldMeta in metaData.FieldMetaCollection)
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

                fs.WriteInt(metaData.FieldNameCollection.Count());
                foreach (var fieldName in metaData.FieldNameCollection)
                {
                    fs.WriteByte(fieldName.Number);
                    fs.WriteString(fieldName.Name);
                }
            }
        }

        public bool IsExist()
        {
            return _fileSystem.FileExists(FileFullPath);
        }

        public void Delete()
        {
            _fileSystem.DeleteFile(FileFullPath);
        }
    }

    internal class MetaData
    {
        public string EntityTypeName { get; set; }
        public Type PrimaryKeyType { get; set; }
        public string PrimaryKeyName { get; set; }
        public IEnumerable<FieldMeta> FieldMetaCollection { get; set; }
        public IEnumerable<FieldName> FieldNameCollection { get; set; }

        public override bool Equals(object obj)
        {
            return obj is MetaData data &&
                   EntityTypeName == data.EntityTypeName &&
                   PrimaryKeyType == data.PrimaryKeyType &&
                   PrimaryKeyName == data.PrimaryKeyName &&
                   (FieldMetaCollection == null && data.FieldMetaCollection == null ||
                   FieldMetaCollection.ToHashSet().IsSubsetOf(data.FieldMetaCollection) && data.FieldMetaCollection.ToHashSet().IsSubsetOf(FieldMetaCollection)) &&
                   (FieldNameCollection == null && data.FieldNameCollection == null ||
                   FieldNameCollection.ToHashSet().IsSubsetOf(data.FieldNameCollection) && data.FieldNameCollection.ToHashSet().IsSubsetOf(FieldNameCollection));
        }

        public override int GetHashCode()
        {
            int hashCode = -860317296;
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(EntityTypeName);
            hashCode = hashCode * -1521134295 + EqualityComparer<Type>.Default.GetHashCode(PrimaryKeyType);
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(PrimaryKeyName);
            hashCode = hashCode * -1521134295 + EqualityComparer<IEnumerable<FieldMeta>>.Default.GetHashCode(FieldMetaCollection);
            hashCode = hashCode * -1521134295 + EqualityComparer<IEnumerable<FieldName>>.Default.GetHashCode(FieldNameCollection);
            return hashCode;
        }

        public static MetaData MakeFromMapper<TEntity>(Mapper<TEntity> mapper)
        {
            return Make(
                mapper.EntityType.Name,
                mapper.PrimaryKeyMapping.PropertyType,
                mapper.PrimaryKeyMapping.PropertyName,
                mapper.FieldMetaCollection,
                mapper.FieldMappings.Select(x => new FieldName(x.Number, x.PropertyName)));
        }

        public static MetaData Make(
            string entityTypeName, Type primaryKeyType, string primaryKeyName, IEnumerable<FieldMeta> fieldMetaCollection, IEnumerable<FieldName> fieldNameCollection)
        {
            return new MetaData
            {
                EntityTypeName = entityTypeName,
                PrimaryKeyType = primaryKeyType,
                PrimaryKeyName = primaryKeyName,
                FieldMetaCollection = fieldMetaCollection,
                FieldNameCollection = fieldNameCollection
            };
        }
    }

    internal class FieldName
    {
        public byte Number { get; }

        public string Name { get; }

        public FieldName(byte number, string name)
        {
            Name = name;
            Number = number;
        }

        public override bool Equals(object obj)
        {
            return obj is FieldName name &&
                   Number == name.Number &&
                   Name == name.Name;
        }

        public override int GetHashCode()
        {
            int hashCode = 453561286;
            hashCode = hashCode * -1521134295 + Number.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(Name);
            return hashCode;
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
