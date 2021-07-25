using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class PrimaryKey
    {
        public object Value { get; private set; }

        public long StartDataFileOffset { get; private set; }

        public long EndDataFileOffset { get; private set; }

        public PrimaryKey(object value, long startDataFileOffset, long endDataFileOffset)
        {
            Value = value;
            StartDataFileOffset = startDataFileOffset;
            EndDataFileOffset = endDataFileOffset;
        }
    }
}
