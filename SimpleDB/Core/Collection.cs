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
                return GetInternal(id);
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
                    yield return GetInternal(id);
                }
            }
            finally
            {
                DataFile.EndReadWrite();
            }
        }

        private TEntity GetInternal(object id)
        {
            if (Exist(id) == false) return default(TEntity);
            var primaryKey = PrimaryKeys[id];
            var fieldNumbers = Mapper.FieldMetaCollection.Select(x => x.Number).ToHashSet();
            var fieldValueCollection = new Dictionary<byte, FieldValue>();
            DataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers, fieldValueCollection);
            var entity = Mapper.GetEntity(primaryKey.Value, fieldValueCollection.Values, true);

            return entity;
        }

        public void Insert(TEntity entity)
        {
            try
            {
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                InsertInternal(entity);
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
                    InsertInternal(entity);
                }
            }
            finally
            {
                DataFile.EndReadWrite();
                PrimaryKeyFile.EndReadWrite();
            }
        }

        private void InsertInternal(TEntity entity)
        {
            var fieldValueCollection = Mapper.GetFieldValueCollection(entity);
            var insertResult = DataFile.Insert(fieldValueCollection);
            var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
            var primaryKey = PrimaryKeyFile.Insert(primaryKeyValue, insertResult.StartDataFileOffset, insertResult.EndDataFileOffset);
            PrimaryKeys.Add(primaryKeyValue, primaryKey);
        }

        public void Update(TEntity entity)
        {
            UpdateInternal(entity);
        }

        public void Update(IEnumerable<TEntity> entities)
        {
            try
            {
                DataFile.BeginWrite();
                PrimaryKeyFile.BeginWrite();
                foreach (var entity in entities)
                {
                    UpdateInternal(entity);
                }
            }
            finally
            {
                DataFile.EndReadWrite();
                PrimaryKeyFile.EndReadWrite();
            }
        }

        private void UpdateInternal(TEntity entity)
        {
            var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
            if (Exist(primaryKeyValue) == false) return;
            var primaryKey = PrimaryKeys[primaryKeyValue];
            var fieldValueCollection = Mapper.GetFieldValueCollection(entity);
            var updateResult = DataFile.Update(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldValueCollection);
            if (primaryKey.StartDataFileOffset != updateResult.NewStartDataFileOffset)
            {
                PrimaryKeyFile.UpdateStartEndDataFileOffset(primaryKey.PrimaryKeyFileOffset, updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset);
                primaryKey.StartDataFileOffset = updateResult.NewStartDataFileOffset;
                primaryKey.EndDataFileOffset = updateResult.NewEndDataFileOffset;
            }
            else if (primaryKey.EndDataFileOffset != updateResult.NewEndDataFileOffset)
            {
                PrimaryKeyFile.UpdateEndDataFileOffset(primaryKey.PrimaryKeyFileOffset, updateResult.NewEndDataFileOffset);
                primaryKey.EndDataFileOffset = updateResult.NewEndDataFileOffset;
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
                    UpdateInternal(entity);
                }
                else
                {
                    InsertInternal(entity);
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
                        UpdateInternal(entity);
                    }
                    else
                    {
                        InsertInternal(entity);
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
                DeleteInternal(id);
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
                    DeleteInternal(id);
                }
            }
            finally
            {
                PrimaryKeyFile.EndReadWrite();
            }
        }

        private void DeleteInternal(object id)
        {
            var primaryKey = PrimaryKeys[id];
            PrimaryKeyFile.Delete(primaryKey.PrimaryKeyFileOffset);
            PrimaryKeys.Remove(id);
        }

        public IQueryable<TEntity> Query()
        {
            var queryExecutor = new QueryExecutor<TEntity>(Mapper, DataFile, PrimaryKeys.Values);
            return new Queryable<TEntity>(queryExecutor, Mapper);
        }
    }
}
