using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Linq;
using SimpleDB.QueryExecutors;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.Core
{
    internal class Collection<TEntity> : ICollection<TEntity>
    {
        private readonly IndexHolder _indexHolder;
        private readonly IndexUpdater _indexUpdater;

        internal Mapper<TEntity> Mapper { get; private set; }

        internal PrimaryKeyFile PrimaryKeyFile { get; private set; }

        internal DataFile DataFile { get; private set; }

        internal Dictionary<object, PrimaryKey> PrimaryKeys { get; private set; }

        public Collection(
            Mapper<TEntity> mapper,
            IPrimaryKeyFileFactory primaryKeyFileFactory,
            IDataFileFactory dataFileFactory,
            IMetaFileFactory metaFileFactory,
            IndexHolder indexHolder = null,
            IndexUpdater indexUpdater = null)
        {
            Mapper = mapper;
            _indexHolder = indexHolder ?? new IndexHolder();
            _indexUpdater = indexUpdater ?? new IndexUpdater(mapper);
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

        public TEntity Get(object id)
        {
            try
            {
                DataFile.BeginRead();
                return EntityOperations.Get(id, Mapper, PrimaryKeys, DataFile);
            }
            finally
            {
                DataFile.EndReadWrite();
            }
        }

        public IEnumerable<TEntity> Get(IEnumerable<object> idList)
        {
            try
            {
                DataFile.BeginRead();
                foreach (var id in idList)
                {
                    yield return EntityOperations.Get(id, Mapper, PrimaryKeys, DataFile);
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
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                EntityOperations.Insert(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                _indexUpdater.AddToIndexes(entity);
            }
            finally
            {
                DataFile.EndReadWrite();
                PrimaryKeyFile.EndReadWrite();
            }
        }

        public void Insert(IEnumerable<TEntity> entities)
        {
            try
            {
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                foreach (var entity in entities)
                {
                    EntityOperations.Insert(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                }
                _indexUpdater.AddToIndexes(entities);
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
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                EntityOperations.Update(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                _indexUpdater.UpdateIndexes(entity);
            }
            finally
            {
                DataFile.EndReadWrite();
                PrimaryKeyFile.EndReadWrite();
            }
        }

        public void Update(IEnumerable<TEntity> entities)
        {
            try
            {
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                foreach (var entity in entities)
                {
                    EntityOperations.Update(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                }
                _indexUpdater.UpdateIndexes(entities);
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
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
                if (Exist(primaryKeyValue))
                {
                    EntityOperations.Update(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                    _indexUpdater.UpdateIndexes(entity);
                }
                else
                {
                    EntityOperations.Insert(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                    _indexUpdater.AddToIndexes(entity);
                }
            }
            finally
            {
                DataFile.EndReadWrite();
                PrimaryKeyFile.EndReadWrite();
            }
        }

        public void InsertOrUpdate(IEnumerable<TEntity> entities)
        {
            try
            {
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                foreach (var entity in entities)
                {
                    var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
                    if (Exist(primaryKeyValue))
                    {
                        EntityOperations.Update(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                        _indexUpdater.UpdateIndexes(entity);
                    }
                    else
                    {
                        EntityOperations.Insert(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
                        _indexUpdater.AddToIndexes(entity);
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
                PrimaryKeyFile.BeginWrite();
                EntityOperations.Delete(id, PrimaryKeyFile, PrimaryKeys);
                _indexUpdater.DeleteFromIndexes<TEntity>(id);
            }
            finally
            {
                PrimaryKeyFile.EndReadWrite();
            }
        }

        public void Delete(IEnumerable<object> idList)
        {
            try
            {
                PrimaryKeyFile.BeginWrite();
                foreach (var id in idList)
                {
                    EntityOperations.Delete(id, PrimaryKeyFile, PrimaryKeys);
                    _indexUpdater.DeleteFromIndexes<TEntity>(idList);
                }
            }
            finally
            {
                PrimaryKeyFile.EndReadWrite();
            }
        }

        public IQueryable<TEntity> Query()
        {
            var queryExecutorFactory = new QueryExecutorFactory<TEntity>(Mapper, PrimaryKeyFile, PrimaryKeys, DataFile, _indexHolder, _indexUpdater);
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
                metaFile.Save(currentMetaData);
            }
        }
    }

    internal interface ICollectionFactory
    {
        Collection<TEntity> Make<TEntity>(Mapper<TEntity> mapper, IndexHolder indexHolder = null, IndexUpdater indexUpdater = null);
    }

    internal class CollectionFactory : ICollectionFactory
    {
        private readonly string _workingDirectory;
        private readonly IFileSystem _fileSystem;
        private readonly IMemory _memory;

        public CollectionFactory(string workingDirectory, IFileSystem fileSystem = null, IMemory memory = null)
        {
            _workingDirectory = workingDirectory;
            _fileSystem = fileSystem ?? FileSystem.Instance;
            _memory = memory ?? Memory.Instance;
        }

        public Collection<TEntity> Make<TEntity>(Mapper<TEntity> mapper, IndexHolder indexHolder = null, IndexUpdater indexUpdater = null)
        {
            return new Collection<TEntity>(
                mapper,
                new PrimaryKeyFileFactory(_workingDirectory, _fileSystem, _memory),
                new DataFileFactory(_workingDirectory, _fileSystem, _memory),
                new MetaFileFactory(_workingDirectory, _fileSystem),
                indexHolder,
                indexUpdater);
        }
    }
}
