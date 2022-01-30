using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Utils;

namespace SimpleDB.QueryExecutors
{
    internal class AnalyzedTreeItem
    {
        public static AnalyzedTreeItem MakeFrom(WhereClause.WhereClauseItem whereClauseItem)
        {
            var item = new AnalyzedTreeItem(whereClauseItem);
            if (whereClauseItem.Left != null) item.Left = MakeFrom(whereClauseItem.Left);
            if (whereClauseItem.Right != null) item.Right = MakeFrom(whereClauseItem.Right);

            return item;
        }

        public static void Replace(ref AnalyzedTreeItem root, AnalyzedTreeItem oldItem, AnalyzedTreeItem newItem)
        {
            if (oldItem.Parent != null)
            {
                if (oldItem.IsLeftChild) oldItem.Parent.Left = newItem;
                else oldItem.Parent.Right = newItem;
                oldItem.Parent = null;
            }
            else
            {
                root = newItem;
                newItem.Parent = null;
            }
        }

        private WhereClause.WhereClauseItem _whereClauseItem;
        private AnalyzedTreeItem _right;
        private AnalyzedTreeItem _left;

        public AnalyzedTreeItem(WhereClause.WhereClauseItem whereClauseItem)
        {
            _whereClauseItem = whereClauseItem;
        }

        public AnalyzedTreeItem Left
        {
            get { return _left; }
            set
            {
                _left = value;
                if (_left != null) _left.Parent = this;
            }
        }

        public AnalyzedTreeItem Right
        {
            get { return _right; }
            set
            {
                _right = value;
                if (_right != null) _right.Parent = this;
            }
        }

        public AnalyzedTreeItem Parent { get; set; }

        public List<FieldValueCollection> IndexResult { get; set; }

        public HashSet<object> PrimaryKeys { get; set; }

        public bool IsNotApplied { get; set; }

        public bool IsFieldOperation
        {
            get { return _whereClauseItem is WhereClause.FieldOperation; }
        }

        public Type OperationType
        {
            get { return _whereClauseItem.GetType(); }
        }

        public byte FieldNumber
        {
            get
            {
                if (_whereClauseItem is WhereClause.PrimaryKey) return PrimaryKey.FieldNumber;
                if (_whereClauseItem is WhereClause.Field) return ((WhereClause.Field)_whereClauseItem).Number;
                if (_whereClauseItem.Left is WhereClause.PrimaryKey) return PrimaryKey.FieldNumber;
                if (_whereClauseItem.Left is WhereClause.Field) return ((WhereClause.Field)_whereClauseItem.Left).Number;
                throw new InvalidOperationException();
            }
        }

        public object ConstantValue
        {
            get
            {
                if (_whereClauseItem is WhereClause.Constant) return ((WhereClause.Constant)_whereClauseItem).Value;
                else if (_whereClauseItem.Right is WhereClause.Constant) return ((WhereClause.Constant)_whereClauseItem.Right).Value;
                else if (_whereClauseItem.Right is WhereClause.Set) return ((WhereClause.Set)_whereClauseItem.Right).Value;
                else throw new InvalidOperationException();
            }
        }

        public bool IsIndexed
        {
            get { return IndexResult != null; }
        }

        public bool AndOperation
        {
            get
            {
                return
                    !IsNotApplied && _whereClauseItem is WhereClause.AndOperation ||
                    IsNotApplied && _whereClauseItem is WhereClause.OrOperation;
            }
        }

        public bool OrOperation
        {
            get
            {
                return
                    !IsNotApplied && _whereClauseItem is WhereClause.OrOperation ||
                    IsNotApplied && _whereClauseItem is WhereClause.AndOperation;
            }
        }

        public bool NotOperation
        {
            get { return _whereClauseItem is WhereClause.NotOperation; }
        }

        public bool IsLeftChild
        {
            get { return Parent != null && Parent.Left == this; }
        }

        public bool IsRightChild
        {
            get { return Parent != null && Parent.Right == this; }
        }

        public AnalyzedTreeItem Sibling
        {
            get
            {
                if (Parent == null) return null;
                return Parent.Left == this ? Parent.Right : Parent.Left;
            }
        }

        public IEnumerable<AnalyzedTreeItem> ToEnumerable()
        {
            return TreeUtils.ToEnumerable(this, n => n.Left, n => n.Right);
        }

        public bool GetValue(FieldValueCollection fieldValueCollection)
        {
            if (IsIndexed)
            {
                return PrimaryKeys.Contains(fieldValueCollection.PrimaryKey.Value);
            }
            else
            {
                var value = (bool)_whereClauseItem.GetValue(fieldValueCollection);
                if (IsNotApplied) value = !value;
                return value;
            }
        }

        public void ApplyAnd(IEnumerable<object> primaryKeys)
        {
            foreach (var item in ToEnumerable().Where(x => x.PrimaryKeys != null))
            {
                item.PrimaryKeys.IntersectWith(primaryKeys);
            }
        }

        public void ApplyOr(IEnumerable<object> primaryKeys)
        {
            foreach (var item in ToEnumerable().Where(x => x.PrimaryKeys != null))
            {
                item.PrimaryKeys.ExceptWith(primaryKeys);
            }
        }

        public bool OnlyOrOperationToRoot
        {
            get
            {
                var item = Parent;
                while (item != null && item.OrOperation) item = item.Parent;
                return item == null;
            }
        }
    }
}
