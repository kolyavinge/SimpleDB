using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.IndexedSearch
{
    internal class Index<TField> : IIndex where TField : IComparable<TField>
    {
        private readonly RBTree<TField, IndexValue> _indexTree;

        public Index(IndexMeta meta)
        {
            _indexTree = new RBTree<TField, IndexValue>();
            Meta = meta;
        }

        private Index(IndexMeta meta, RBTree<TField, IndexValue> indexTree)
        {
            _indexTree = indexTree;
            Meta = meta;
        }

        public IndexMeta Meta { get; private set; }

        public IndexValue GetEquals(object fieldValue)
        {
            return _indexTree.Find((TField)fieldValue)?.Value;
        }

        public IEnumerable<IndexValue> GetNotEquals(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
            {
                if (step.ToLeft)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllNodesAsc());
                }
                else if (step.ToRight)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllNodesAsc());
                }
                else if (step.Finded)
                {
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllNodesAsc());
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllNodesAsc());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetLess(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
            {
                if (step.ToRight)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllNodesAsc());
                }
                else if (step.Finded)
                {
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllNodesAsc());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetGreat(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
            {
                if (step.ToLeft)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllNodesAsc());
                }
                else if (step.Finded)
                {
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllNodesAsc());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetLessOrEquals(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
            {
                if (step.ToRight)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllNodesAsc());
                }
                else if (step.Finded)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllNodesAsc());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetGreatOrEquals(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
            {
                if (step.ToLeft)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllNodesAsc());
                }
                else if (step.Finded)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllNodesAsc());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetLike(object fieldValue)
        {
            return _indexTree.Root.GetAllNodesAsc().Where(x => x.Key.ToString().Contains(fieldValue.ToString())).Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetNotLike(object fieldValue)
        {
            return _indexTree.Root.GetAllNodesAsc().Where(x => !x.Key.ToString().Contains(fieldValue.ToString())).Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetIn(IEnumerable<object> fieldValues)
        {
            return fieldValues.Select(GetEquals);
        }

        public IEnumerable<IndexValue> GetNotIn(IEnumerable<object> fieldValues)
        {
            var set = fieldValues.ToHashSet();
            return _indexTree.Root.GetAllNodesAsc().Where(x => !set.Contains(x.Key)).Select(x => x.Value);
        }

        public void Add(object indexedFieldValue, IndexItem indexItem)
        {
            var node = _indexTree.InsertOrGetExists((TField)indexedFieldValue);
            if (node.Value == null)
            {
                node.Value = new IndexValue { IndexedFieldValue = indexedFieldValue, Items = new List<IndexItem> { indexItem } };
            }
            else
            {
                node.Value.Items.Add(indexItem);
            }
        }

        public void Add(object indexedFieldValue, IEnumerable<IndexItem> indexItems)
        {
            var node = _indexTree.InsertOrGetExists((TField)indexedFieldValue);
            if (node.Value == null)
            {
                node.Value = new IndexValue { IndexedFieldValue = indexedFieldValue, Items = indexItems.ToList() };
            }
            else
            {
                node.Value.Items.AddRange(indexItems);
            }
        }

        public IEnumerable<IndexValue> GetAllIndexValues(SortDirection sortDirection = SortDirection.Asc)
        {
            if (sortDirection == SortDirection.Asc)
            {
                return _indexTree.Root.GetAllNodesAsc().Select(x => x.Value);
            }
            else
            {
                return _indexTree.Root.GetAllNodesDesc().Select(x => x.Value);
            }
        }

        public void Delete(object indexedFieldValue)
        {
            _indexTree.Delete((TField)indexedFieldValue);
        }

        public void Clear()
        {
            _indexTree.Clear();
        }

        public void Serialize(IWriteableStream stream)
        {
            Meta.Serialize(stream);
            var rbTreeSerializer = new RBTreeSerializer<TField, IndexValue>(new IndexNodeSerializer<TField>());
            rbTreeSerializer.Serialize(_indexTree, stream);
        }

        public static Index<TField> Deserialize(IReadableStream stream, Type primaryKeyType, IDictionary<byte, Type> fieldTypes)
        {
            var indexMeta = IndexMeta.Deserialize(stream);
            return Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
        }

        public static Index<TField> Deserialize(IReadableStream stream, IndexMeta indexMeta, Type primaryKeyType, IDictionary<byte, Type> fieldTypes)
        {
            var rbTreeSerializer = new RBTreeSerializer<TField, IndexValue>(new IndexNodeSerializer<TField>(indexMeta, primaryKeyType, fieldTypes));
            var indexTree = rbTreeSerializer.Deserialize(stream);

            return new Index<TField>(indexMeta, indexTree);
        }
    }

    internal static class PrimitiveTypeIndex
    {
        public static IIndex Deserialize(IReadableStream stream, Type primaryKeyType, IDictionary<byte, Type> fieldTypes)
        {
            var indexMeta = IndexMeta.Deserialize(stream);
            var fieldType = indexMeta.IndexedFieldType;
            if (fieldType == null) throw new ArgumentException($"IndexedFieldType cannot be null");
            if (fieldType == typeof(bool)) return Index<bool>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(sbyte)) return Index<sbyte>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(byte)) return Index<byte>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(char)) return Index<char>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(short)) return Index<short>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(ushort)) return Index<ushort>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(int)) return Index<int>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(uint)) return Index<uint>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(long)) return Index<long>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(ulong)) return Index<ulong>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(float)) return Index<float>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(double)) return Index<double>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(decimal)) return Index<decimal>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(DateTime)) return Index<DateTime>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            if (fieldType == typeof(string)) return Index<string>.Deserialize(stream, indexMeta, primaryKeyType, fieldTypes);
            throw new ArgumentException($"Cannot read index for type '{fieldType}'");
        }
    }
}
