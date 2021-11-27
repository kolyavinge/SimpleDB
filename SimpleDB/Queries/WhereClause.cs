using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Utils;
using SimpleDB.Utils.EnumerableExtension;

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

        public IEnumerable<byte> GetAllFieldNumbers()
        {
            return Root.ToEnumerable().OfType<Field>().Select(x => x.Number).Distinct();
        }

        public abstract class WhereClauseItem
        {
            private WhereClauseItem _left;
            private WhereClauseItem _right;

            public WhereClauseItem Parent { get; private set; }

            public WhereClauseItem Left
            {
                get { return _left; }
                set
                {
                    _left = value;
                    _left.Parent = this;
                }
            }

            public WhereClauseItem Right
            {
                get { return _right; }
                set
                {
                    _right = value;
                    _right.Parent = this;
                }
            }

            public WhereClauseItem GetSibling()
            {
                if (Parent == null) return null;
                return Parent.Left == this ? Parent.Right : Parent.Left;
            }

            public IEnumerable<WhereClauseItem> ToEnumerable()
            {
                return TreeUtils.ToEnumerable(this, n => n.Left, n => n.Right);
            }

            public abstract object GetValue(FieldValueCollection fieldValueCollection);
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
                var leftValue = Left.GetValue(fieldValueCollection);
                var rightValue = Right.GetValue(fieldValueCollection);
                return SmartComparer.AreEquals(leftValue, rightValue);
            }

            public override string ToString()
            {
                return String.Format("Equals({0}, {1})", Left, Right);
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

            public override string ToString()
            {
                return String.Format("Not({0})", Left);
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

            public override string ToString()
            {
                return String.Format("And({0}, {1})", Left, Right);
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

            public override string ToString()
            {
                return String.Format("Or({0}, {1})", Left, Right);
            }
        }

        public class LessOperation : FieldOperation
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

            public override string ToString()
            {
                return String.Format("Less({0}, {1})", Left, Right);
            }
        }

        public class GreatOperation : FieldOperation
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

            public override string ToString()
            {
                return String.Format("Great({0}, {1})", Left, Right);
            }
        }

        public class LessOrEqualsOperation : FieldOperation
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

            public override string ToString()
            {
                return String.Format("LessOrEquals({0}, {1})", Left, Right);
            }
        }

        public class GreatOrEqualsOperation : FieldOperation
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

            public override string ToString()
            {
                return String.Format("GreatOrEquals({0}, {1})", Left, Right);
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
                var leftValue = (string)Left.GetValue(fieldValueCollection);
                var rightValue = (string)Right.GetValue(fieldValueCollection);
                return leftValue.Contains(rightValue);
            }

            public override string ToString()
            {
                return String.Format("Like({0}, {1})", Left, Right);
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
                var leftValue = Left.GetValue(fieldValueCollection);
                var set = (ISet<object>)Right.GetValue(fieldValueCollection);
                return set.Contains(leftValue);
            }

            public override string ToString()
            {
                return String.Format("In({0}, {1})", Left, Right);
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
                return String.Format("PrimaryKey()");
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

            public override string ToString()
            {
                return String.Format("Field({0})", Number);
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
                    return String.Format("'{0}'", Value);
                }
                else
                {
                    return String.Format("{0}", Value);
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
                return String.Format("Set({0})", String.Join(",", Value));
            }
        }
    }
}
