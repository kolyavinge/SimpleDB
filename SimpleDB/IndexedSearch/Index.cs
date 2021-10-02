using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch
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

        public override IndexValue GetEquals(object fieldValue)
        {
            var node = _indexTree.Find((TField)fieldValue);
            return node != null ? node.Value : null;
        }

        public override IEnumerable<IndexValue> GetNotEquals(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
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

        public override IEnumerable<IndexValue> GetLess(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
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

        public override IEnumerable<IndexValue> GetGreat(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
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

        public override IEnumerable<IndexValue> GetLessOrEquals(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
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

        public override IEnumerable<IndexValue> GetGreatOrEquals(object fieldValue)
        {
            var nodes = new List<RBTree<TField, IndexValue>.Node>();
            foreach (var step in new RBTreeFindNodeEnumerable<TField, IndexValue>(_indexTree.Root, (TField)fieldValue))
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

        public override IEnumerable<IndexValue> GetLike(object fieldValue)
        {
            return _indexTree.Root.GetAllChildren().Where(x => x.Key.ToString().Contains(fieldValue.ToString())).Select(x => x.Value);
        }

        public override IEnumerable<IndexValue> GetNotLike(object fieldValue)
        {
            return _indexTree.Root.GetAllChildren().Where(x => !x.Key.ToString().Contains(fieldValue.ToString())).Select(x => x.Value);
        }

        public override IEnumerable<IndexValue> GetIn(IEnumerable<object> fieldValues)
        {
            return fieldValues.Select(GetEquals);
        }

        public override IEnumerable<IndexValue> GetNotIn(IEnumerable<object> fieldValues)
        {
            var set = fieldValues.ToHashSet();
            return _indexTree.Root.GetAllChildren().Where(x => !set.Contains(x.Key)).Select(x => x.Value);
        }

        public override void Add(object indexedFieldValue, IndexItem indexItem)
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

        public override void Serialize(IWriteableStream stream)
        {
            Meta.Serialize(stream);
            var rbTreeSerializer = new RBTreeSerializer<TField, IndexValue>(new IndexNodeSerializer<TField>());
            rbTreeSerializer.Serialize(_indexTree, stream);
        }
    }
}
