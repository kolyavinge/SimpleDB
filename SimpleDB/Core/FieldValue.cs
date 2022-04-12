using System;
using System.Collections.Generic;

namespace SimpleDB.Core
{
    internal class FieldValue : IEquatable<FieldValue>
    {
        public byte Number { get; }

        public object? Value { get; }

        public FieldValue(byte number, object? value)
        {
            if (number == 0) throw new ArgumentException("Number must be greater than zero");
            Number = number;
            Value = value;
        }

        public bool Equals(FieldValue obj)
        {
            return Equals((object)obj);
        }

        public override bool Equals(object obj)
        {
            return obj is FieldValue fieldValue &&
                   Number == fieldValue.Number &&
                   EqualityComparer<object>.Default.Equals(Value!, fieldValue.Value!);
        }

        public override int GetHashCode()
        {
            int hashCode = -596618710;
            hashCode = hashCode * -1521134295 + Number.GetHashCode();
            hashCode = hashCode * -1521134295 + EqualityComparer<object>.Default.GetHashCode(Value!);
            return hashCode;
        }
    }
}
