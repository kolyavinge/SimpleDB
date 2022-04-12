using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Utils;

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
            var value = Root.GetValue(fieldValueCollection);
            if (value == null) throw new DBEngineException("WhereClause incorrect");
            return (bool)value;
        }

        public IEnumerable<byte> GetAllFieldNumbers()
        {
            return Root.ToEnumerable().OfType<Field>().Select(x => x.Number).Distinct();
        }

        public abstract class WhereClauseItem
        {
            private WhereClauseItem? _left;
            private WhereClauseItem? _right;

            public WhereClauseItem? Parent { get; private set; }

            public WhereClauseItem? Left
            {
                get => _left;
                set
                {
                    _left = value;
                    if (_left != null) _left.Parent = this;
                }
            }

            public WhereClauseItem? Right
            {
                get => _right;
                set
                {
                    _right = value;
                    if (_right != null) _right.Parent = this;
                }
            }

            public WhereClauseItem? GetSibling()
            {
                if (Parent == null) return null;
                return Parent.Left == this ? Parent.Right : Parent.Left;
            }

            public IEnumerable<WhereClauseItem> ToEnumerable()
            {
                return TreeUtils.ToEnumerable(this, n => n.Left, n => n.Right);
            }

            public abstract object? GetValue(FieldValueCollection fieldValueCollection);
        }

        public abstract class FieldOperation : WhereClauseItem
        {
        }

        public class EqualsOperation : FieldOperation
        {
            public EqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                return SmartComparer.AreEquals(leftValue, rightValue);
            }

            public override string ToString()
            {
                return $"Equals({Left}, {Right})";
            }
        }

        public class NotOperation : WhereClauseItem
        {
            public NotOperation(WhereClauseItem left)
            {
                Left = left;
            }

            public override object? GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                if (leftValue == null) throw new DBEngineException("WhereClause incorrect");
                return (bool)leftValue == false;
            }

            public override string ToString()
            {
                return $"Not({Left})";
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
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                if (leftValue == null) throw new DBEngineException("WhereClause incorrect");
                if (rightValue == null) throw new DBEngineException("WhereClause incorrect");

                return (bool)leftValue && (bool)rightValue;
            }

            public override string ToString()
            {
                return $"And({Left}, {Right})";
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
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                if (leftValue == null) throw new DBEngineException("WhereClause incorrect");
                if (rightValue == null) throw new DBEngineException("WhereClause incorrect");

                return (bool)leftValue || (bool)rightValue;
            }

            public override string ToString()
            {
                return $"Or({Left}, {Right})";
            }
        }

        public class LessOperation : FieldOperation
        {
            public LessOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object? GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) < 0;
            }

            public override string ToString()
            {
                return $"Less({Left}, {Right})";
            }
        }

        public class GreatOperation : FieldOperation
        {
            public GreatOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object? GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) > 0;
            }

            public override string ToString()
            {
                return $"Great({Left}, {Right})";
            }
        }

        public class LessOrEqualsOperation : FieldOperation
        {
            public LessOrEqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object? GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) < 0 || SmartComparer.Compare(leftValue, rightValue) == 0;
            }

            public override string ToString()
            {
                return $"LessOrEquals({Left}, {Right})";
            }
        }

        public class GreatOrEqualsOperation : FieldOperation
        {
            public GreatOrEqualsOperation(WhereClauseItem left, WhereClauseItem right)
            {
                Left = left;
                Right = right;
            }

            public override object? GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                return SmartComparer.Compare(leftValue, rightValue) > 0 || SmartComparer.Compare(leftValue, rightValue) == 0;
            }

            public override string ToString()
            {
                return $"GreatOrEquals({Left}, {Right})";
            }
        }

        public class LikeOperation : FieldOperation
        {
            public LikeOperation(WhereClauseItem left, Constant right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                var rightValue = Right!.GetValue(fieldValueCollection);
                if (leftValue == null) throw new DBEngineException("WhereClause incorrect");
                if (rightValue == null) throw new DBEngineException("WhereClause incorrect");

                return ((string)leftValue).Contains((string)rightValue);
            }

            public override string ToString()
            {
                return $"Like({Left}, {Right})";
            }
        }

        public class InOperation : FieldOperation
        {
            public InOperation(WhereClauseItem left, Set right)
            {
                Left = left;
                Right = right;
            }

            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                var leftValue = Left!.GetValue(fieldValueCollection);
                var set = Right!.GetValue(fieldValueCollection) as ISet<object?>;
                if (set == null) throw new DBEngineException("WhereClause incorrect");

                return set.Contains(leftValue);
            }

            public override string ToString()
            {
                return $"In({Left}, {Right})";
            }
        }

        public class PrimaryKey : WhereClauseItem
        {
            public override object GetValue(FieldValueCollection fieldValueCollection)
            {
                return fieldValueCollection.PrimaryKey.Value;
            }

            public override string ToString()
            {
                return "PrimaryKey()";
            }
        }

        public class Field : WhereClauseItem
        {
            public Field(byte number)
            {
                Number = number;
            }

            public byte Number { get; }

            public override object? GetValue(FieldValueCollection fieldValueCollection)
            {
                return fieldValueCollection[Number].Value;
            }

            public override string ToString()
            {
                return $"Field({Number})";
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

            public override string ToString()
            {
                if (Value is string)
                {
                    return $"'{Value}'";
                }
                else
                {
                    return $"{Value}";
                }
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

            public override string ToString()
            {
                return $"Set({String.Join(",", Value)})";
            }
        }
    }
}
