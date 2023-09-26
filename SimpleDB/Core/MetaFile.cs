using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core;

internal class MetaFile
{
    private readonly IFileSystem _fileSystem;

    public string FileName { get; }

    public MetaFile(string fileName, IFileSystem fileSystem)
    {
        FileName = fileName;
        _fileSystem = fileSystem;
    }

    public MetaData GetMetaData()
    {
        using (var fs = _fileSystem.OpenFileRead(FileName))
        {
            var entityName = fs.ReadString();

            var primaryKeyFieldType = (FieldTypes)fs.ReadByte();
            var primaryKeyType = primaryKeyFieldType == FieldTypes.Object ? Type.GetType(fs.ReadString()) : FieldTypesConverter.GetType(primaryKeyFieldType);

            var primaryKeyName = fs.ReadString();

            var fieldMetaCollection = new List<FieldMeta>();
            int fieldMetaCollectionCount = fs.ReadInt();
            for (int i = 0; i < fieldMetaCollectionCount; i++)
            {
                var number = fs.ReadByte();
                var name = fs.ReadString();
                var fieldType = (FieldTypes)fs.ReadByte();
                var type = fieldType == FieldTypes.Object ? Type.GetType(fs.ReadString()) : FieldTypesConverter.GetType(fieldType);
                var compressed = fs.ReadBool();
                fieldMetaCollection.Add(new FieldMeta(number, name, type) { Settings = new FieldSettings { Compressed = compressed } });
            }

            return new MetaData(entityName, primaryKeyType, primaryKeyName, fieldMetaCollection);
        }
    }

    public void Save(MetaData metaData)
    {
        using (var fs = _fileSystem.OpenFileReadWrite(FileName))
        {
            fs.WriteString(metaData.EntityName);

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
                fs.WriteString(fieldMeta.Name);
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

    public bool IsExist()
    {
        return _fileSystem.FileExists(FileName);
    }

    public void Create()
    {
        _fileSystem.CreateFiles(FileName);
    }

    public void Delete()
    {
        _fileSystem.DeleteFile(FileName);
    }
}

internal class MetaData
{
    public string EntityName { get; set; }
    public Type PrimaryKeyType { get; set; }
    public string PrimaryKeyName { get; set; }
    public IEnumerable<FieldMeta> FieldMetaCollection { get; set; }

    public MetaData(string entityName, Type primaryKeyType, string primaryKeyName, IEnumerable<FieldMeta> fieldMetaCollection)
    {
        EntityName = entityName;
        PrimaryKeyType = primaryKeyType;
        PrimaryKeyName = primaryKeyName;
        FieldMetaCollection = fieldMetaCollection;
    }

    public override bool Equals(object obj)
    {
        return obj is MetaData data &&
               EntityName == data.EntityName &&
               PrimaryKeyType == data.PrimaryKeyType &&
               PrimaryKeyName == data.PrimaryKeyName &&
               (FieldMetaCollection is null && data.FieldMetaCollection is null ||
               FieldMetaCollection.ToHashSet().IsSubsetOf(data.FieldMetaCollection) && data.FieldMetaCollection.ToHashSet().IsSubsetOf(FieldMetaCollection));
    }

    public override int GetHashCode()
    {
        int hashCode = -860317296;
        hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(EntityName);
        hashCode = hashCode * -1521134295 + EqualityComparer<Type>.Default.GetHashCode(PrimaryKeyType);
        hashCode = hashCode * -1521134295 + EqualityComparer<string>.Default.GetHashCode(PrimaryKeyName);
        hashCode = hashCode * -1521134295 + EqualityComparer<IEnumerable<FieldMeta>>.Default.GetHashCode(FieldMetaCollection);
        return hashCode;
    }

    public static MetaData MakeFromMapper<TEntity>(Mapper<TEntity> mapper)
    {
        return new MetaData(
            mapper.EntityName,
            mapper.PrimaryKeyMapping.PropertyType,
            mapper.PrimaryKeyMapping.PropertyName,
            mapper.FieldMetaCollection);
    }
}

internal interface IMetaFileFactory
{
    MetaFile MakeFromEntityName(string entityName);
}

internal class MetaFileFactory : IMetaFileFactory
{
    private readonly IFileSystem _fileSystem;

    public MetaFileFactory(IFileSystem fileSystem)
    {
        _fileSystem = fileSystem;
    }

    public MetaFile MakeFromEntityName(string entityName)
    {
        return new MetaFile(MetaFileName.FromEntityName(entityName), _fileSystem);
    }
}

internal static class MetaFileName
{
    public const string Extension = ".meta";

    public static string FromEntityName(string entityName)
    {
        return $"{entityName}{Extension}";
    }
}
