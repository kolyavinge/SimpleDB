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
            IEnumerable<TEntity> entities, Mapper<TEntity> mapper, PrimaryKeyFile primaryKeyFile, DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys)
        {
            var insertedPrimaryKeys = new List<KeyValuePair<object, DataFile.InsertResult>>();

            foreach (var entity in entities)
            {
                var fieldValueCollection = mapper.GetFieldValueCollection(entity);
                var insertResult = dataFile.Insert(fieldValueCollection);
                var primaryKeyValue = mapper.GetPrimaryKeyValue(entity);
                insertedPrimaryKeys.Add(new KeyValuePair<object, DataFile.InsertResult>(primaryKeyValue, insertResult));
            }

            foreach (var item in insertedPrimaryKeys)
            {
                var primaryKeyValue = item.Key;
                var insertResult = item.Value;
                var primaryKey = primaryKeyFile.Insert(primaryKeyValue, insertResult.StartDataFileOffset, insertResult.EndDataFileOffset);
                primaryKeys.Add(primaryKeyValue, primaryKey);
            }
        }

        public static void Update<TEntity>(
            IEnumerable<TEntity> entities, Mapper<TEntity> mapper, PrimaryKeyFile primaryKeyFile, DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys)
        {
            var updatedPrimaryKeys = new List<KeyValuePair<PrimaryKey, DataFile.UpdateResult>>();

            foreach (var entity in entities)
            {
                var primaryKeyValue = mapper.GetPrimaryKeyValue(entity);
                if (primaryKeys.ContainsKey(primaryKeyValue) == false) break;
                var primaryKey = primaryKeys[primaryKeyValue];
                var fieldValueCollection = mapper.GetFieldValueCollection(entity);
                var updateResult = dataFile.Update(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, fieldValueCollection);
                updatedPrimaryKeys.Add(new KeyValuePair<PrimaryKey, DataFile.UpdateResult>(primaryKey, updateResult));
            }

            foreach (var item in updatedPrimaryKeys)
            {
                var primaryKey = item.Key;
                var updateResult = item.Value;
                primaryKeyFile.UpdatePrimaryKey(primaryKey, updateResult.NewStartDataFileOffset, updateResult.NewEndDataFileOffset);
            }
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
