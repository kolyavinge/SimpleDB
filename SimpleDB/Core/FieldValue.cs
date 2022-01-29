using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class FieldValue : IEquatable<FieldValue>
    {
        public byte Number { get; private set; }

        public object Value { get; private set; }

        public FieldValue(byte number, object value)
        {
            Number = number;
            Value = value;
        }

        public bool Equals(FieldValue obj)
        {
            return Equals((object)obj);
        }

        public override bool Equals(object obj)
        {
            return obj is FieldValue value &&
                   Number == value.Number &&
                   EqualityComparer<object>.Default.Equals(Value, value.Value);
        }

        public override int GetHashCode()
        {
            int hashCode = -596618710;
            hashCode = hashCode * -1521134295 + Number.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<object>.Default.GetHashCode(Value);
            return hashCode;
        }
    }
}
