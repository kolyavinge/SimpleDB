using System;
using System.Collections.Generic;

namespace SimpleDB.Queries
{
    internal class WhereClause
    {
        public WhereClause(WhereClauseItem root)
        {
            Root = root;
        }

        public WhereClauseItem Root { get; }

        public bool GetValue(IDictionary<byte, object> fieldValueDictionary)
        {
            var value = (bool)Root.GetValue(fieldValueDictionary);
            return value;
        }

        public IEnumerable<WhereClauseItem> ToEnumerable()
        {
            var items = new List<WhereClauseItem>();
            Action<WhereClauseItem> rec = null;
            rec = (WhereClauseItem parent) =>
            {
                items.Add(parent);
                if (parent.Left != null) rec(parent.Left);
                if (parent.Right != null) rec(parent.Right);
            };
            rec(Root);

            return items;
        }

        public abstract class WhereClauseItem
        {
            public WhereClauseItem Left { get; protected set; }

            public WhereClauseItem Right { get; protected set; }

            public abstract object GetValue(IDictionary<byte, object> fieldValueDictionary);
        }

        public class EqualsOperation : WhereClauseItem
        {
            public EqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                var leftValue = Left.GetValue(fieldValueDictionary);
                var rightValue = Right.GetValue(fieldValueDictionary);
                return leftValue.Equals(rightValue);
            }
        }

        public class LessOperation : WhereClauseItem
        {
            public LessOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                var leftValue = (IComparable)Left.GetValue(fieldValueDictionary);
                var rightValue = (IComparable)Right.GetValue(fieldValueDictionary);
                return leftValue.CompareTo(rightValue) < 0;
            }
        }

        public class GreatOperation : WhereClauseItem
        {
            public GreatOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                var leftValue = (IComparable)Left.GetValue(fieldValueDictionary);
                var rightValue = (IComparable)Right.GetValue(fieldValueDictionary);
                return leftValue.CompareTo(rightValue) > 0;
            }
        }

        public class LessOrEqualsOperation : WhereClauseItem
        {
            public LessOrEqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                var leftValue = (IComparable)Left.GetValue(fieldValueDictionary);
                var rightValue = (IComparable)Right.GetValue(fieldValueDictionary);
                return leftValue.CompareTo(rightValue) < 0 || leftValue.CompareTo(rightValue) == 0;
            }
        }

        public class GreatOrEqualsOperation : WhereClauseItem
        {
            public GreatOrEqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                var leftValue = (IComparable)Left.GetValue(fieldValueDictionary);
                var rightValue = (IComparable)Right.GetValue(fieldValueDictionary);
                return leftValue.CompareTo(rightValue) > 0 || leftValue.CompareTo(rightValue) == 0;
            }
        }

        public class NotOperation : WhereClauseItem
        {
            public NotOperation(WhereClauseItem left)
            {
                Left = left;
            }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                var leftValue = (bool)Left.GetValue(fieldValueDictionary);
                return leftValue == false;
            }
        }

        public class LikeOperation : WhereClauseItem
        {
            public LikeOperation(WhereClauseItem left, Constant right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                var leftValue = (string)Left.GetValue(fieldValueDictionary);
                var rightValue = (string)Right.GetValue(fieldValueDictionary);
                return leftValue.Contains(rightValue);
            }
        }

        public class Field : WhereClauseItem
        {
            public Field(byte number)
            {
                Number = number;
            }

            public byte Number { get; }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                return fieldValueDictionary[Number];
            }
        }

        public class Constant : WhereClauseItem
        {
            public Constant(object value)
            {
                Value = value;
            }

            public object Value { get; }

            public override object GetValue(IDictionary<byte, object> fieldValueDictionary)
            {
                return Value;
            }
        }
    }
}
