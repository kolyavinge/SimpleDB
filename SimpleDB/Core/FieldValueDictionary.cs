using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Core
{
    internal class FieldValueDictionary
    {
        public PrimaryKey PrimaryKey { get; set; }

        public Dictionary<byte, FieldValue> FieldValues { get; set; }

        public FieldValueDictionary()
        {
            FieldValues = new Dictionary<byte, FieldValue>();
        }
    }
}
