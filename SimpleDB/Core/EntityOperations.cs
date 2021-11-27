using System.Collections.Generic;
using System.Linq;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.Core
{
    internal static class EntityOperations
    {
        public static TEntity Get<TEntity>(object id, Mapper<TEntity> mapper, IDictionary<object, PrimaryKey> primaryKeys, DataFile dataFile)
        {
            if (primaryKeys.ContainsKey(id) == false) return default(TEntity);
            var primaryKey = primaryKeys[id];
            var fieldNumbers = mapper.FieldMetaCollection.Select(x => x.Number).ToHashSet();
            var fieldValueCollection = new FieldValueCollection();
            dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldNumbers, fieldValueCollection);
            var entity = mapper.MakeEntity(primaryKey.Value, fieldValueCollection, true);

            return entity;
        }

        public static void Insert<TEntity>(
            TEntity entity, Mapper<TEntity> mapper, PrimaryKeyFile primaryKeyFile, DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys)
        {
            var fieldValueCollection = mapper.GetFieldValueCollection(entity);
            var insertResult = dataFile.Insert(fieldValueCollection);
            var primaryKeyValue = mapper.GetPrimaryKeyValue(entity);
            var primaryKey = primaryKeyFile.Insert(primaryKeyValue, insertResult.StartDataFileOffset, insertResult.EndDataFileOffset);
            primaryKeys.Add(primaryKeyValue, primaryKey);
        }

        public static void Update<TEntity>(
            TEntity entity, Mapper<TEntity> mapper, PrimaryKeyFile primaryKeyFile, DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys)
        {
            var primaryKeyValue = mapper.GetPrimaryKeyValue(entity);
            if (primaryKeys.ContainsKey(primaryKeyValue) == false) return;
            var primaryKey = primaryKeys[primaryKeyValue];
            var fieldValueCollection = mapper.GetFieldValueCollection(entity);
            var updateResult = dataFile.Update(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldValueCollection);
            primaryKeyFile.UpdatePrimaryKey(primaryKey, updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset);
        }

        public static void UpdateAllFields(FieldValueCollection fieldValueCollection, PrimaryKeyFile primaryKeyFile, DataFile dataFile)
        {
            var primaryKey = fieldValueCollection.PrimaryKey;
            var updateResult = dataFile.Update(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldValueCollection);
            primaryKeyFile.UpdatePrimaryKey(primaryKey, updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset);
        }

        public static void Delete(object id, PrimaryKeyFile primaryKeyFile, IDictionary<object, PrimaryKey> primaryKeys)
        {
            var primaryKey = primaryKeys[id];
            primaryKeyFile.Delete(primaryKey.PrimaryKeyFileOffset);
            primaryKeys.Remove(id);
        }
    }
}
