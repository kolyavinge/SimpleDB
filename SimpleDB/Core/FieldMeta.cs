using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class FieldMeta
    {
        public byte Number { get; private set; }

        public Type Type { get; private set; }

        public FieldMeta(byte number, Type type)
        {
            Number = number;
            Type = type;
        }
    }
}
