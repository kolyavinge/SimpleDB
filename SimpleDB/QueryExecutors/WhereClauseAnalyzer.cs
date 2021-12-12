using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal class WhereClauseAnalyzer
    {
        private readonly string _entityName;
        private readonly IDictionary<object, PrimaryKey> _primaryKeys;
        private readonly IFieldValueReader _fieldValueReader;
        private readonly IndexHolder _indexHolder;
        private List<FieldValueCollection> _indexCache;

        public WhereClauseAnalyzer(string entityName, IDictionary<object, PrimaryKey> primaryKeys, IFieldValueReader fieldValueReader, IndexHolder indexHolder)
        {
            _entityName = entityName;
            _primaryKeys = primaryKeys;
            _fieldValueReader = fieldValueReader;
            _indexHolder = indexHolder;
            _indexCache = new List<FieldValueCollection>();
        }

        public IEnumerable<FieldValueCollection> GetResult(WhereClause whereClause)
        {
            var root = AnalyzedTreeItem.MakeFrom(whereClause.Root);
            ApplyNot(ref root);
            SearchIndexes(root);
            List<FieldValueCollection> partialResult = null;
            ProcessTree(ref root, ref partialResult);
            var treeResult = GetTreeResult(root);

            return FieldValueCollection.Union(partialResult, treeResult);
        }

        private void ApplyNot(ref AnalyzedTreeItem root)
        {
            foreach (var item in root.ToEnumerable().Where(x => x.NotOperation).ToList())
            {
                item.ToEnumerable().Each(x => x.IsNotApplied = !x.IsNotApplied);
                AnalyzedTreeItem.Replace(ref root, item, item.Left);
            }
        }

        private void SearchIndexes(AnalyzedTreeItem root)
        {
            foreach (var item in root.ToEnumerable().Where(x => x.IsFieldOperation))
            {
                List<FieldValueCollection> fieldValueCollections = null;
                // ищем значение поля среди проиндексированных
                var indexResult = _indexHolder.GetIndexResult(item.OperationType, item.IsNotApplied, _entityName, item.FieldNumber, item.ConstantValue);
                if (indexResult != null)
                {
                    fieldValueCollections = indexResult.SelectMany(x => x.ToFieldValueCollections(_primaryKeys)).ToList();
                }
                else
                {
                    // если нету таких, сканируем индекс целиком
                    fieldValueCollections = _indexHolder
                        .GetScanResult(_entityName, _primaryKeys.Keys, _primaryKeys, new[] { item.FieldNumber })
                        .Where(item.GetValue)
                        .ToList();
                }
                if (fieldValueCollections.Any())
                {
                    item.IndexResult = fieldValueCollections;
                    item.PrimaryKeys = fieldValueCollections.Select(x => x.PrimaryKey.Value).ToHashSet();
                    _indexCache = FieldValueCollection.Union(_indexCache, fieldValueCollections).ToList();
                }
                else
                {
                    item.PrimaryKeys = _primaryKeys.Keys.ToHashSet();
                }
            }
        }

        private void ProcessTree(ref AnalyzedTreeItem root, ref List<FieldValueCollection> partialResult)
        {
            var itemsToProcess = new Queue<AnalyzedTreeItem>(root.ToEnumerable().Where(x => x.IsIndexed));
            while (itemsToProcess.Any())
            {
                var item = itemsToProcess.Dequeue();
                if (item.Parent == null) continue;
                var sibling = item.Sibling;
                if (item.Parent.AndOperation)
                {
                    sibling.ApplyAnd(item.PrimaryKeys);
                    if (item.IsIndexed && sibling.IsIndexed)
                    {
                        item.Parent.PrimaryKeys = sibling.PrimaryKeys;
                        item.Parent.IndexResult = FieldValueCollection.Union(item.IndexResult, sibling.IndexResult).ToList();
                        itemsToProcess.Enqueue(item.Parent);
                        item.Parent.Left = null;
                        item.Parent.Right = null;
                        item.Parent = null;
                        sibling.Parent = null;
                    }
                    else if (item.IsIndexed && !sibling.IsIndexed)
                    {
                        AnalyzedTreeItem.Replace(ref root, item.Parent, sibling);
                        itemsToProcess.Enqueue(sibling);
                    }
                }
                else if (item.Parent.OrOperation)
                {
                    if (item.IsIndexed && sibling.IsIndexed)
                    {
                        item.Parent.PrimaryKeys = item.PrimaryKeys.Union(sibling.PrimaryKeys).ToHashSet();
                        item.Parent.IndexResult = FieldValueCollection.Union(item.IndexResult, sibling.IndexResult).ToList();
                        itemsToProcess.Enqueue(item.Parent);
                        item.Parent.Left = null;
                        item.Parent.Right = null;
                        item.Parent = null;
                        sibling.Parent = null;
                    }
                    else if (item.IsIndexed && !sibling.IsIndexed)
                    {
                        sibling.ApplyOr(item.PrimaryKeys);
                        if (item.OnlyOrOperationToRoot)
                        {
                            var indexResult = item.IndexResult.Where(x => item.PrimaryKeys.Contains(x.PrimaryKey.Value));
                            partialResult = FieldValueCollection.Union(partialResult, indexResult).ToList();
                            AnalyzedTreeItem.Replace(ref root, item.Parent, sibling);
                            itemsToProcess.Enqueue(sibling);
                        }
                    }
                }
            }
        }

        private IEnumerable<FieldValueCollection> GetTreeResult(AnalyzedTreeItem root)
        {
            var fieldValueCollections = root.ToEnumerable().Where(x => x.IsIndexed).SelectMany(x => x.IndexResult).ToDictionary(k => k.PrimaryKey.Value, v => v);
            var fieldNumbers = root.ToEnumerable()
                .Where(x => !x.IsIndexed && x.PrimaryKeys != null)
                .Select(x => x.FieldNumber)
                .ToHashSet();
            if (fieldNumbers.Any())
            {
                var primaryKeyValues = root.ToEnumerable().Where(x => !x.IsIndexed && x.PrimaryKeys != null).SelectMany(x => x.PrimaryKeys).ToHashSet();
                foreach (var primaryKeyValue in primaryKeyValues.Where(pk => !fieldValueCollections.ContainsKey(pk)))
                {
                    fieldValueCollections.Add(primaryKeyValue, new FieldValueCollection { PrimaryKey = _primaryKeys[primaryKeyValue] });
                }
                _fieldValueReader.ReadFieldValues(fieldValueCollections.Values, fieldNumbers);
            }
            var result = fieldValueCollections.Values.Where(root.GetValue);
            result = FieldValueCollection.Merge(result, _indexCache);

            return result;
        }
    }
}
