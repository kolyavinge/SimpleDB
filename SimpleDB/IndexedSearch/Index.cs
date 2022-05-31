using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.DataStructures;
using SimpleDB.Infrastructure;

namespace SimpleDB.IndexedSearch;

internal class Index<TField> : IIndex where TField : IComparable<TField>
{
    private readonly RBTree<TField, IndexValue> _indexTree;

    public Index(IndexMeta meta)
    {
        _indexTree = new RBTree<TField, IndexValue>();
        Meta = meta;
    }

    public Index(IndexMeta meta, RBTree<TField, IndexValue> indexTree)
    {
        _indexTree = indexTree;
        Meta = meta;
    }

    public IndexMeta Meta { get; }

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
            node.Value = new IndexValue(indexedFieldValue, new List<IndexItem> { indexItem });
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
            node.Value = new IndexValue(indexedFieldValue, indexItems.ToList());
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
}
