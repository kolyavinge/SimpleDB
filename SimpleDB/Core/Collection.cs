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
            var allPrimaryKeys = PrimaryKeyFile.GetAllPrimaryKeys().ToList();
            PrimaryKeys = allPrimaryKeys.Where(x => !x.IsDeleted()).ToDictionary(k => k.Value, v => v);
        }

        public void Dispose()
        {
            PrimaryKeyFile.Dispose();
            DataFile.Dispose();
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
            if (PrimaryKeys.ContainsKey(id))
            {
                var primaryKey = PrimaryKeys[id];
                var fieldNumbers = Mapper.FieldMetaCollection.Select(x => x.Number).ToHashSet();
                var fieldValueCollection = new Dictionary<byte, FieldValue>();
                DataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers, fieldValueCollection);
                var entity = Mapper.GetEntity(primaryKey.Value, fieldValueCollection.Values, true);

                return entity;
            }
            else
            {
                return default(TEntity);
            }
        }

        public void Insert(TEntity entity)
        {
            var fieldValueCollection = Mapper.GetFieldValueCollection(entity);
            var insertResult = DataFile.Insert(fieldValueCollection);
            var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
            var primaryKey = PrimaryKeyFile.Insert(primaryKeyValue, insertResult.StartDataFileOffset, insertResult.EndDataFileOffset);
            PrimaryKeys.Add(primaryKeyValue, primaryKey);
        }

        public void Update(TEntity entity)
        {
            var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
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
            var primaryKeyValue = Mapper.GetPrimaryKeyValue(entity);
            if (Exist(primaryKeyValue))
            {
                Update(entity);
            }
            else
            {
                Insert(entity);
            }
        }

        public void Delete(object id)
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
