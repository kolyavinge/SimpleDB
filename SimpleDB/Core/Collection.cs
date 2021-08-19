using System.Collections.Generic;
using System.IO;
using System.Linq;
using SimpleDB.Linq;

namespace SimpleDB.Core
{
    internal class Collection<TEntity> : ICollection<TEntity>
    {
        internal Mapper<TEntity> Mapper { get; private set; }

        internal PrimaryKeyFile PrimaryKeyFile { get; private set; }

        internal DataFile DataFile { get; private set; }

        internal Dictionary<object, PrimaryKey> PrimaryKeys { get; private set; }

        public Collection(string workingDirectory, Mapper<TEntity> mapper)
        {
            Mapper = mapper;
            var primaryKeyFileFullPath = Path.Combine(workingDirectory, PrimaryKeyFileName.FromCollectionName(mapper.EntityName));
            var dataFileFileFullPath = Path.Combine(workingDirectory, DataFileFileName.FromCollectionName(mapper.EntityName));
            PrimaryKeyFile = new PrimaryKeyFile(primaryKeyFileFullPath, mapper.PrimaryKeyMapping.PropertyType);
            DataFile = new DataFile(dataFileFileFullPath, mapper.FieldMetaCollection);
            PrimaryKeys = GetAllPrimaryKeys().Where(x => !x.IsDeleted()).ToDictionary(k => k.Value, v => v);
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
            var queryExecutorFactory = new QueryExecutorFactory<TEntity>(Mapper, PrimaryKeyFile, PrimaryKeys, DataFile);
            return new Queryable<TEntity>(queryExecutorFactory, Mapper);
        }
    }
}
