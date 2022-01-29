using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal class UpdateQueryExecutor
    {
        private readonly EntityMeta _entityMeta;
        private readonly PrimaryKeyFile _primaryKeyFile;
        private readonly DataFile _dataFile;
        private readonly IDictionary<object, PrimaryKey> _primaryKeys;
        private readonly IndexHolder _indexHolder;
        private readonly IIndexUpdater _indexUpdater;

        public UpdateQueryExecutor(
            EntityMeta entityMeta, PrimaryKeyFile primaryKeyFile, DataFile dataFile, IDictionary<object, PrimaryKey> primaryKeys, IndexHolder indexHolder, IIndexUpdater indexUpdater)
        {
            _entityMeta = entityMeta;
            _primaryKeyFile = primaryKeyFile;
            _dataFile = dataFile;
            _primaryKeys = primaryKeys;
            _indexHolder = indexHolder;
            _indexUpdater = indexUpdater;
        }

        public int ExecuteQuery(UpdateQuery query)
        {
            try
            {
                _primaryKeyFile.BeginReadWrite();
                _dataFile.BeginReadWrite();
                return TryExecuteQuery(query);
            }
            finally
            {
                _primaryKeyFile.EndReadWrite();
                _dataFile.EndReadWrite();
            }
        }

        private int TryExecuteQuery(UpdateQuery query)
        {
            var fieldValueCollections = new List<FieldValueCollection>();
            var allFieldNumbers = new HashSet<byte>();
            // where
            if (query.WhereClause != null)
            {
                var whereFieldNumbers = query.WhereClause.GetAllFieldNumbers().ToHashSet();
                if (_indexHolder.AnyIndexContainsFields(query.EntityName, whereFieldNumbers))
                {
                    var analyzer = new WhereClauseAnalyzer(query.EntityName, _primaryKeys, new FieldValueReader(_dataFile), _indexHolder);
                    fieldValueCollections.AddRange(analyzer.GetResult(query.WhereClause));
                    allFieldNumbers.AddRange(fieldValueCollections.SelectMany(collection => collection.Select(field => field.Number)));
                }
                else
                {

                    allFieldNumbers.AddRange(whereFieldNumbers);
                    foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
                    {
                        var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKey };
                        _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, whereFieldNumbers, fieldValueCollection);
                        var whereResult = query.WhereClause.GetValue(fieldValueCollection);
                        if (whereResult)
                        {
                            fieldValueCollections.Add(fieldValueCollection);
                        }
                    }
                }
            }
            else
            {
                foreach (var primaryKey in _primaryKeys.Values.OrderBy(x => x.StartDataFileOffset))
                {
                    var fieldValueCollection = new FieldValueCollection { PrimaryKey = primaryKey };
                    fieldValueCollections.Add(fieldValueCollection);
                }
            }
            // update
            var updateFieldDictionary = query.UpdateClause.UpdateItems.Cast<UpdateClause.Field>().ToDictionary(k => k.Number, v => new FieldValue(v.Number, v.Value));
            var updateFieldNumbers = query.UpdateClause.GetAllFieldNumbers().ToHashSet();
            var variableFieldNumbers =
                (from meta in _entityMeta.FieldMetaCollection
                 join fieldNumber in updateFieldNumbers on meta.Number equals fieldNumber
                 let fieldType = FieldTypesConverter.GetFieldType(meta.Type)
                 where fieldType == FieldTypes.String || fieldType == FieldTypes.ByteArray || fieldType == FieldTypes.Object
                 select fieldNumber).ToList();
            // если среди обновляемых есть поля с переменной длиной
            if (variableFieldNumbers.Any())
            {
                var nonSelectedUpdateFieldNumbers = updateFieldNumbers.ToHashSet();
                nonSelectedUpdateFieldNumbers.ExceptWith(allFieldNumbers);
                allFieldNumbers.AddRange(nonSelectedUpdateFieldNumbers);
                var remainingFieldNumbers = _entityMeta.FieldMetaCollection.Select(x => x.Number).ToHashSet();
                remainingFieldNumbers.ExceptWith(allFieldNumbers);
                if (nonSelectedUpdateFieldNumbers.Any())
                {
                    // добираем недостающие значения
                    foreach (var fieldValueCollection in fieldValueCollections)
                    {
                        var primaryKey = fieldValueCollection.PrimaryKey;
                        _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, nonSelectedUpdateFieldNumbers, fieldValueCollection);
                    }
                }
                foreach (var fieldValueCollection in fieldValueCollections)
                {
                    var primaryKey = fieldValueCollection.PrimaryKey;
                    foreach (var variableFieldNumber in variableFieldNumbers)
                    {
                        var currentValue = fieldValueCollection[variableFieldNumber].Value;
                        var currentValueByteArray = _dataFile.ToByteArray(variableFieldNumber, currentValue);
                        var newValue = updateFieldDictionary[variableFieldNumber].Value;
                        var newValueByteArray = _dataFile.ToByteArray(variableFieldNumber, newValue);
                        // проверяем чтобы старое и новое значение в байтах равнялись по длине
                        if (currentValueByteArray.Length != newValueByteArray.Length)
                        {
                            // если не равняются, добираем все значения до конца
                            _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, remainingFieldNumbers, fieldValueCollection);
                            foreach (var updateFieldValue in updateFieldDictionary.Values)
                            {
                                // обновляем их
                                fieldValueCollection[updateFieldValue.Number] = new FieldValue(updateFieldValue.Number, updateFieldValue.Value);
                            }
                            // делаем полное обновление всей записи
                            EntityOperations.UpdateAllFields(fieldValueCollection, _primaryKeyFile, _dataFile);
                            break;
                        }
                    }
                    _dataFile.UpdateManual(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, updateFieldDictionary.Values);
                }
            }
            else
            {
                foreach (var fieldValueCollection in fieldValueCollections)
                {
                    var primaryKey = fieldValueCollection.PrimaryKey;
                    _dataFile.UpdateManual(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, updateFieldDictionary.Values);
                }
            }

            _indexUpdater.UpdateIndexes(_entityMeta, fieldValueCollections.Select(x => x.PrimaryKey.Value), updateFieldDictionary.Values);

            return fieldValueCollections.Count;
        }
    }
}
