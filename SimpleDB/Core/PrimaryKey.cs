using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class PrimaryKey
    {
        public object Value { get; private set; }

        public long StartDataFileOffset { get; set; }

        public long EndDataFileOffset { get; set; }

        public long PrimaryKeyFileOffset { get; set; }

        public PrimaryKey(object value, long startDataFileOffset, long endDataFileOffset, long primaryKeyFileOffset)
        {
            Value = value;
            StartDataFileOffset = startDataFileOffset;
            EndDataFileOffset = endDataFileOffset;
            PrimaryKeyFileOffset = primaryKeyFileOffset;
        }
    }
}
