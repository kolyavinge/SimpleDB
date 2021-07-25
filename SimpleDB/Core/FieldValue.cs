using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class FieldValue
    {
        public byte Number { get; private set; }

        public object Value { get; private set; }

        public FieldValue(byte number, object value)
        {
            Number = number;
            Value = value;
        }
    }
}
