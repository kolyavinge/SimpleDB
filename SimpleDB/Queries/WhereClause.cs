using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;

namespace SimpleDB.Queries
{
    internal class WhereClause
    {
        public WhereClause(WhereClauseItem root)
        {
            Root = root;
        }

        public WhereClauseItem Root { get; }

        public bool GetValue(FieldValueDictionary fieldValueDictionary)
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

        public IEnumerable<byte> GetAllFieldNumbers()
        {
            return ToEnumerable().Where(x => x is Field).Cast<Field>().Select(x => x.Number).Distinct();
        }

        public abstract class WhereClauseItem
        {
            public WhereClauseItem Left { get; protected set; }

            public WhereClauseItem Right { get; protected set; }

            public abstract object GetValue(FieldValueDictionary fieldValueDictionary);
        }

        public class EqualsOperation : WhereClauseItem
        {
            public EqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                var leftValue = Left.GetValue(fieldValueDictionary);
                var rightValue = Right.GetValue(fieldValueDictionary);
                return leftValue.Equals(rightValue);
            }
        }

        public class NotOperation : WhereClauseItem
        {
            public NotOperation(WhereClauseItem left)
            {
                Left = left;
            }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                var leftValue = (bool)Left.GetValue(fieldValueDictionary);
                return leftValue == false;
            }
        }

        public class AndOperation : WhereClauseItem
        {
            public AndOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                var leftValue = (bool)Left.GetValue(fieldValueDictionary);
                var rightValue = (bool)Right.GetValue(fieldValueDictionary);
                return leftValue && rightValue;
            }
        }

        public class OrOperation : WhereClauseItem
        {
            public OrOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                var leftValue = (bool)Left.GetValue(fieldValueDictionary);
                var rightValue = (bool)Right.GetValue(fieldValueDictionary);
                return leftValue || rightValue;
            }
        }

        public class LessOperation : WhereClauseItem
        {
            public LessOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
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

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
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

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
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

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                var leftValue = (IComparable)Left.GetValue(fieldValueDictionary);
                var rightValue = (IComparable)Right.GetValue(fieldValueDictionary);
                return leftValue.CompareTo(rightValue) > 0 || leftValue.CompareTo(rightValue) == 0;
            }
        }

        public class LikeOperation : WhereClauseItem
        {
            public LikeOperation(WhereClauseItem left, Constant right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                var leftValue = (string)Left.GetValue(fieldValueDictionary);
                var rightValue = (string)Right.GetValue(fieldValueDictionary);
                return leftValue.Contains(rightValue);
            }
        }

        public class PrimaryKey : WhereClauseItem
        {
            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                return fieldValueDictionary.PrimaryKey.Value;
            }
        }

        public class Field : WhereClauseItem
        {
            public Field(byte number)
            {
                Number = number;
            }

            public byte Number { get; }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                return fieldValueDictionary.FieldValues[Number].Value;
            }
        }

        public class Constant : WhereClauseItem
        {
            public Constant(object value)
            {
                Value = value;
            }

            public object Value { get; }

            public override object GetValue(FieldValueDictionary fieldValueDictionary)
            {
                return Value;
            }
        }
    }
}
