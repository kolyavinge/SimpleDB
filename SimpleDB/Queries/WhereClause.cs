﻿using System;
using System.Collections;
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

        public bool GetValue(FieldValueCollection fieldValueCollection)
        {
            var value = (bool)Root.GetValue(fieldValueCollection);
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
            return ToEnumerable().OfType<Field>().Select(x => x.Number).Distinct();
        }

        public abstract class WhereClauseItem
        {
            public WhereClauseItem Left { get; protected set; }

            public WhereClauseItem Right { get; protected set; }

            public abstract object GetValue(FieldValueCollection fieldValueCollection);
        }

        public class EqualsOperation : WhereClauseItem
        {
            public EqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left.GetValue(fieldValueCollection);
                var rightValue = Right.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) == 0;
            }
        }

        public class NotOperation : WhereClauseItem
        {
            public NotOperation(WhereClauseItem left)
            {
                Left = left;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = (bool)Left.GetValue(fieldValueCollection);
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

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = (bool)Left.GetValue(fieldValueCollection);
                var rightValue = (bool)Right.GetValue(fieldValueCollection);
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

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = (bool)Left.GetValue(fieldValueCollection);
                var rightValue = (bool)Right.GetValue(fieldValueCollection);
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

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left.GetValue(fieldValueCollection);
                var rightValue = Right.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) < 0;
            }
        }

        public class GreatOperation : WhereClauseItem
        {
            public GreatOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left.GetValue(fieldValueCollection);
                var rightValue = Right.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) > 0;
            }
        }

        public class LessOrEqualsOperation : WhereClauseItem
        {
            public LessOrEqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left.GetValue(fieldValueCollection);
                var rightValue = Right.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) < 0 || SmartComparer.Compare(leftValue, rightValue) == 0;
            }
        }

        public class GreatOrEqualsOperation : WhereClauseItem
        {
            public GreatOrEqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left.GetValue(fieldValueCollection);
                var rightValue = Right.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) > 0 || SmartComparer.Compare(leftValue, rightValue) == 0;
            }
        }

        public class LikeOperation : WhereClauseItem
        {
            public LikeOperation(WhereClauseItem left, Constant right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = (string)Left.GetValue(fieldValueCollection);
                var rightValue = (string)Right.GetValue(fieldValueCollection);
                return leftValue.Contains(rightValue);
            }
        }

        public class InOperation : WhereClauseItem
        {
            public InOperation(WhereClauseItem left, Set right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left.GetValue(fieldValueCollection);
                var set = (ISet<object>)Right.GetValue(fieldValueCollection);
                return set.Contains(leftValue);
            }
        }

        public class PrimaryKey : WhereClauseItem
        {
            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                return fieldValueCollection.PrimaryKey.Value;
            }
        }

        public class Field : WhereClauseItem
        {
            public Field(byte number)
            {
                Number = number;
            }

            public byte Number { get; }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                return fieldValueCollection[Number].Value;
            }
        }

        public class Constant : WhereClauseItem
        {
            public Constant(object value)
            {
                Value = value;
            }

            public object Value { get; }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                return Value;
            }
        }

        public class Set : WhereClauseItem
        {
            public Set(IEnumerable value)
            {
                Value = value.Cast<object>().ToHashSet();
            }

            public ISet<object> Value { get; }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                return Value;
            }
        }
    }
}
