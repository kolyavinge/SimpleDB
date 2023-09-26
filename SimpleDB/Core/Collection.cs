using System.Collections.Generic;
using System.Linq;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Linq;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Core;

internal class Collection<TEntity> : ICollection<TEntity>
{
    private readonly IndexHolder _indexHolder;
    private readonly IIndexUpdater _indexUpdater;

    internal Mapper<TEntity> Mapper { get; }

    internal PrimaryKeyFile PrimaryKeyFile { get; }

    internal DataFile DataFile { get; }

    internal Dictionary<object, PrimaryKey> PrimaryKeys { get; }

    public Collection(
        Mapper<TEntity> mapper,
        IPrimaryKeyFileFactory primaryKeyFileFactory,
        IDataFileFactory dataFileFactory,
        IMetaFileFactory metaFileFactory,
        IndexHolder? indexHolder = null,
        IIndexUpdater? indexUpdater = null)
    {
        Mapper = mapper;
        _indexHolder = indexHolder ?? new IndexHolder();
        _indexUpdater = indexUpdater ?? new IndexUpdater(Enumerable.Empty<IIndex>(), null);
        PrimaryKeyFile = primaryKeyFileFactory.MakeFromEntityName(mapper.EntityName, mapper.PrimaryKeyMapping.PropertyType);
        DataFile = dataFileFactory.MakeFromEntityName(mapper.EntityName, mapper.FieldMetaCollection);
        PrimaryKeys = GetAllPrimaryKeys().Where(x => !x.IsDeleted).ToDictionary(k => k.Value, v => v);
        SaveMetaFileIfNeeded(metaFileFactory);
    }

    private List<PrimaryKey> GetAllPrimaryKeys()
    {
        try
        {
            PrimaryKeyFile.BeginRead();
            return PrimaryKeyFile.GetAllPrimaryKeys().ToList();
        }
        finally
        {
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public int Count()
    {
        return PrimaryKeys.Count;
    }

    public bool Exist(object id)
    {
        return PrimaryKeys.ContainsKey(id);
    }

    public TEntity? Get(object id)
    {
        try
        {
            DataFile.BeginRead();
            return EntityOperations.GetOrDefault(id, Mapper, PrimaryKeys, DataFile);
        }
        finally
        {
            DataFile.EndReadWrite();
        }
    }

    public IEnumerable<TEntity> GetRange(IReadOnlyCollection<object> idList)
    {
        try
        {
            DataFile.BeginRead();
            foreach (var id in idList)
            {
                var entity = EntityOperations.GetOrDefault(id, Mapper, PrimaryKeys, DataFile);
                if (entity is not null) yield return entity;
            }
        }
        finally
        {
            DataFile.EndReadWrite();
        }
    }

    public IEnumerable<TEntity> GetAll()
    {
        try
        {
            DataFile.BeginRead();
            foreach (var id in PrimaryKeys.Keys)
            {
                var entity = EntityOperations.GetOrDefault(id, Mapper, PrimaryKeys, DataFile);
                if (entity is not null) yield return entity;
            }
        }
        finally
        {
            DataFile.EndReadWrite();
        }
    }

    public void Insert(TEntity entity)
    {
        try
        {
            DataFile.BeginReadWrite();
            PrimaryKeyFile.BeginReadWrite();
            EntityOperations.Insert(new[] { entity }, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
            _indexUpdater.AddToIndexes(Mapper, new[] { entity });
        }
        finally
        {
            DataFile.EndReadWrite();
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public void InsertRange(IReadOnlyCollection<TEntity> entities)
    {
        try
        {
            DataFile.BeginReadWrite();
            PrimaryKeyFile.BeginReadWrite();
            EntityOperations.Insert(entities, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
            _indexUpdater.AddToIndexes(Mapper, entities);
        }
        finally
        {
            DataFile.EndReadWrite();
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public void Update(TEntity entity)
    {
        try
        {
            DataFile.BeginReadWrite();
            PrimaryKeyFile.BeginReadWrite();
            EntityOperations.Update(new[] { entity }, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
            _indexUpdater.UpdateIndexes(Mapper, new[] { entity });
        }
        finally
        {
            DataFile.EndReadWrite();
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public void UpdateRange(IReadOnlyCollection<TEntity> entities)
    {
        try
        {
            DataFile.BeginReadWrite();
            PrimaryKeyFile.BeginReadWrite();
            EntityOperations.Update(entities, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
            _indexUpdater.UpdateIndexes(Mapper, entities);
        }
        finally
        {
            DataFile.EndReadWrite();
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public void InsertOrUpdate(TEntity entity)
    {
        try
        {
            DataFile.BeginReadWrite();
            PrimaryKeyFile.BeginReadWrite();
            var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
            if (Exist(primaryKeyValue))
            {
                EntityOperations.Update(new[] { entity }, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                _indexUpdater.UpdateIndexes(Mapper, new[] { entity });
            }
            else
            {
                EntityOperations.Insert(new[] { entity }, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                _indexUpdater.AddToIndexes(Mapper, new[] { entity });
            }
        }
        finally
        {
            DataFile.EndReadWrite();
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public void InsertOrUpdateRange(IReadOnlyCollection<TEntity> entities)
    {
        try
        {
            DataFile.BeginReadWrite();
            PrimaryKeyFile.BeginReadWrite();
            foreach (var entity in entities)
            {
                var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
                if (Exist(primaryKeyValue))
                {
                    EntityOperations.Update(new[] { entity }, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                    _indexUpdater.UpdateIndexes(Mapper, new[] { entity });
                }
                else
                {
                    EntityOperations.Insert(new[] { entity }, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                    _indexUpdater.AddToIndexes(Mapper, new[] { entity });
                }
            }
        }
        finally
        {
            DataFile.EndReadWrite();
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public void Delete(object id)
    {
        try
        {
            PrimaryKeyFile.BeginReadWrite();
            EntityOperations.Delete(id, PrimaryKeyFile, PrimaryKeys);
            _indexUpdater.DeleteFromIndexes(Mapper.EntityMeta, new[] { id });
        }
        finally
        {
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public void DeleteRange(IReadOnlyCollection<object> idList)
    {
        try
        {
            PrimaryKeyFile.BeginReadWrite();
            foreach (var id in idList)
            {
                EntityOperations.Delete(id, PrimaryKeyFile, PrimaryKeys);
                _indexUpdater.DeleteFromIndexes(Mapper.EntityMeta, idList);
            }
        }
        finally
        {
            PrimaryKeyFile.EndReadWrite();
        }
    }

    public IQueryable<TEntity> Query()
    {
        var queryExecutorFactory = new QueryExecutorFactory(Mapper.EntityMeta, PrimaryKeyFile, PrimaryKeys, DataFile, _indexHolder, _indexUpdater);
        return new Queryable<TEntity>(queryExecutorFactory, Mapper);
    }

    private void SaveMetaFileIfNeeded(IMetaFileFactory metaFileFactory)
    {
        var currentMetaData = MetaData.MakeFromMapper(Mapper);
        var metaFile = metaFileFactory.MakeFromEntityName(Mapper.EntityName);
        if (metaFile.IsExist())
        {
            var savedMetaData = metaFile.GetMetaData();
            if (!currentMetaData.Equals(savedMetaData))
            {
                metaFile.Delete();
                metaFile.Save(currentMetaData);
            }
        }
        else
        {
            metaFile.Create();
            metaFile.Save(currentMetaData);
        }
    }
}

internal interface ICollectionFactory
{
    Collection<TEntity> Make<TEntity>(Mapper<TEntity> mapper, IndexHolder? indexHolder = null, IIndexUpdater? indexUpdater = null);
}

internal class CollectionFactory : ICollectionFactory
{
    private readonly IFileSystem _fileSystem;
    private readonly IMemory _memory;

    public CollectionFactory(IFileSystem fileSystem, IMemory? memory = null)
    {
        _fileSystem = fileSystem;
        _memory = memory ?? Memory.Instance;
    }

    public Collection<TEntity> Make<TEntity>(Mapper<TEntity> mapper, IndexHolder? indexHolder = null, IIndexUpdater? indexUpdater = null)
    {
        return new Collection<TEntity>(
            mapper,
            new PrimaryKeyFileFactory(_fileSystem, _memory),
            new DataFileFactory(_fileSystem, _memory),
            new MetaFileFactory(_fileSystem),
            indexHolder,
            indexUpdater);
    }
}
