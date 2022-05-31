using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;

namespace SimpleDB.Sql;

internal static class SqlQueryExecutorFactory
{
    public static ISqlQueryExecutor Make(string databaseFilePath)
    {
        var fileSystem = new FileSystem(databaseFilePath);
        var entityMetaDictionary = ReadEntityMetaCollection(fileSystem).ToDictionary(k => k.EntityName, v => v);
        var indexes = ReadIndexes(entityMetaDictionary, fileSystem).ToList();

        return new SqlQueryExecutor(
            entityMetaDictionary,
            new PrimaryKeyFileFactory(fileSystem),
            new DataFileFactory(fileSystem),
            new IndexHolder(indexes),
            new IndexUpdater(indexes, new IndexFileFactory(fileSystem)));
    }

    private static IEnumerable<EntityMeta> ReadEntityMetaCollection(IFileSystem fileSystem)
    {
        var metaFileFactory = new MetaFileFactory(fileSystem);
        foreach (var metaFileName in fileSystem.GetFiles(MetaFileName.Extension))
        {
            var entityName = Path.GetFileNameWithoutExtension(metaFileName);
            var metaFile = metaFileFactory.MakeFromEntityName(entityName);
            var metaData = metaFile.GetMetaData();
            yield return EntityMeta.MakeFromMetaData(metaData);
        }
    }

    private static IEnumerable<IIndex> ReadIndexes(Dictionary<string, EntityMeta> entityMetaDictionary, IFileSystem fileSystem)
    {
        var indexFileFactory = new IndexFileFactory(fileSystem);
        foreach (var indexFileName in fileSystem.GetFiles(IndexFileName.Extension))
        {
            (string entityName, string indexName) = IndexFileName.GetEntityAndIndexName(indexFileName);
            var entityMeta = entityMetaDictionary[entityName];
            var indexFile = indexFileFactory.Make(indexFileName, entityMeta.PrimaryKeyFieldMeta.Type, entityMeta.FieldMetaCollection);
            var index = indexFile.ReadIndex();
            yield return index;
        }
    }
}
