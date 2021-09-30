using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class IndexValue
    {
        public object IndexedFieldValue { get; set; }
        public List<IndexItem> Items { get; set; }
    }

    internal class IndexItem
    {
        public object PrimaryKeyValue { get; set; }
        public object[] IncludedFields { get; set; }
    }

    internal class Index<TField> : AbstractIndex where TField : IComparable<TField>
    {
        private RBTree<TField, IndexValue> _indexTree;

        public Index(IndexMeta meta) : base(meta)
        {
            _indexTree = new RBTree<TField, IndexValue>();
        }

        private Index(IndexMeta meta, RBTree<TField, IndexValue> indexTree) : base(meta)
        {
            _indexTree = indexTree;
        }

        public IndexValue GetEquals(TField fieldValue)
        {
            var node = _indexTree.Find(fieldValue);
            return node != null ? node.Value : null;
        }

        public IEnumerable<IndexValue> GetNotEquals(TField fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, fieldValue))
            {
                if (step.ToLeft)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllChildren());
                }
                else if (step.ToRight)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllChildren());
                }
                else if (step.Finded)
                {
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllChildren());
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllChildren());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetLess(TField fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, fieldValue))
            {
                if (step.ToRight)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllChildren());
                }
                else if (step.Finded)
                {
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllChildren());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetGreat(TField fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, fieldValue))
            {
                if (step.ToLeft)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllChildren());
                }
                else if (step.Finded)
                {
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllChildren());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetLessOrEquals(TField fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, fieldValue))
            {
                if (step.ToRight)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllChildren());
                }
                else if (step.Finded)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Left != null) nodes.AddRange(step.Node.Left.GetAllChildren());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetGreatOrEquals(TField fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, fieldValue))
            {
                if (step.ToLeft)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllChildren());
                }
                else if (step.Finded)
                {
                    nodes.Add(step.Node);
                    if (step.Node.Right != null) nodes.AddRange(step.Node.Right.GetAllChildren());
                }
            }

            return nodes.Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetLike(string fieldValue)
        {
            return _indexTree.Root.GetAllChildren().Where(x => x.Key.ToString().Contains(fieldValue)).Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetNotLike(string fieldValue)
        {
            return _indexTree.Root.GetAllChildren().Where(x => !x.Key.ToString().Contains(fieldValue)).Select(x => x.Value);
        }

        public IEnumerable<IndexValue> GetIn(IEnumerable<TField> fieldValues)
        {
            return fieldValues.Select(GetEquals);
        }

        public IEnumerable<IndexValue> GetNotIn(IEnumerable<TField> fieldValues)
        {
            var set = fieldValues.ToHashSet();
            return _indexTree.Root.GetAllChildren().Where(x => !set.Contains(x.Key)).Select(x => x.Value);
        }

        public void Add(TField indexedFieldValue, IndexItem indexItem)
        {
            var node = _indexTree.InsertOrGetExists(indexedFieldValue);
            if (node.Value == null)
            {
                node.Value = new IndexValue { IndexedFieldValue = indexedFieldValue, Items = new List<IndexItem> { indexItem } };
            }
            else
            {
                node.Value.Items.Add(indexItem);
            }
        }

        public void Add(TField indexedFieldValue, List<IndexItem> indexItems)
        {
            var node = _indexTree.InsertOrGetExists(indexedFieldValue);
            if (node.Value == null)
            {
                node.Value = new IndexValue { IndexedFieldValue = indexedFieldValue, Items = indexItems };
            }
            else
            {
                node.Value.Items.AddRange(indexItems);
            }
        }

        public void Delete(TField fieldValue)
        {
            _indexTree.Delete(fieldValue);
        }

        public void Clear()
        {
            _indexTree.Clear();
        }

        public static Index<TField> Deserialize(IReadableStream stream, Type primaryKeyType, IDictionary<byte, Type> fieldTypes)
        {
            var indexMeta = IndexMeta.Deserialize(stream);
            var rbTreeSerializer = new RBTreeSerializer<TField, IndexValue>(new IndexNodeSerializer<TField>(indexMeta, primaryKeyType, fieldTypes));
            var indexTree = rbTreeSerializer.Deserialize(stream);

            return new Index<TField>(indexMeta, indexTree);
        }

        public void Serialize(IWriteableStream stream)
        {
            Meta.Serialize(stream);
            var rbTreeSerializer = new RBTreeSerializer<TField, IndexValue>(new IndexNodeSerializer<TField>());
            rbTreeSerializer.Serialize(_indexTree, stream);
        }
    }
}
