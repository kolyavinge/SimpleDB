using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;
using SimpleDB.Linq;
using SimpleDB.QueryExecutors;

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

        public Collection(Mapper<TEntity> mapper, IndexHolder indexHolder = null, IndexUpdater indexUpdater = null)
        {
            Mapper = mapper;
            _indexHolder = indexHolder ?? new IndexHolder();
            _indexUpdater = indexUpdater;
            var primaryKeyFileFullPath = Path.Combine(GlobalSettings.WorkingDirectory, PrimaryKeyFileName.FromEntityName(mapper.EntityName));
            var dataFileFileFullPath = Path.Combine(GlobalSettings.WorkingDirectory, DataFileName.FromEntityName(mapper.EntityName));
            PrimaryKeyFile = new PrimaryKeyFile(primaryKeyFileFullPath, mapper.PrimaryKeyMapping.PropertyType);
            DataFile = new DataFile(dataFileFileFullPath, mapper.FieldMetaCollection);
            PrimaryKeys = GetAllPrimaryKeys().Where(x => !x.IsDeleted).ToDictionary(k => k.Value, v => v);
            SaveMetaFileIfNeeded();
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
            EntityOperations.Update(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
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
                }
                else
                {
                    EntityOperations.Insert(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
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
                    }
                    else
                    {
                        EntityOperations.Insert(entity, Mapper, PrimaryKeyFile, DataFile, PrimaryKeys);
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
                }
            }
            finally
            {
                PrimaryKeyFile.EndReadWrite();
            }
        }

        public IQueryable<TEntity> Query()
        {
            var queryExecutorFactory = new QueryExecutorFactory<TEntity>(Mapper, PrimaryKeyFile, PrimaryKeys, DataFile, _indexHolder);
            return new Queryable<TEntity>(queryExecutorFactory, Mapper);
        }

        private void SaveMetaFileIfNeeded()
        {
            var metaFileFullPath = Path.Combine(GlobalSettings.WorkingDirectory, MetaFileName.FromEntityName(Mapper.EntityName));
            var metaFile = new MetaFile(metaFileFullPath);
            if (IOC.Get<IFileSystem>().FileExists(metaFileFullPath))
            {
                var savedFieldMetaCollection = metaFile.GetFieldMetaCollection().ToHashSet();
                if (Mapper.FieldMetaCollection.Count != savedFieldMetaCollection.Count ||
                    !Mapper.FieldMetaCollection.All(savedFieldMetaCollection.Contains))
                {
                    IOC.Get<IFileSystem>().DeleteFile(metaFileFullPath);
                    metaFile.Save(Mapper.PrimaryKeyMapping.PropertyType, Mapper.FieldMetaCollection);
                }
            }
            else
            {
                metaFile.Save(Mapper.PrimaryKeyMapping.PropertyType, Mapper.FieldMetaCollection);
            }
        }
    }
}
