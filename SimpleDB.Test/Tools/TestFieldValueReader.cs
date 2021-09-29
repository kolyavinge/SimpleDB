using System.Collections.Generic;
using SimpleDB.Core;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Test.Tools
{
    internal class TestFieldValueReader : IFieldValueReader
    {
        private readonly FieldValueReader _fieldValueReader;

        public TestFieldValueReader(FieldValueReader fieldValueReader)
        {
            _fieldValueReader = fieldValueReader;
            CallsCount = 0;
        }

        public void ReadFieldValues(IEnumerable<FieldValueCollection> fieldValueCollections, IEnumerable<byte> fieldNumbers)
        {
            _fieldValueReader.ReadFieldValues(fieldValueCollections, fieldNumbers);
            CallsCount++;
        }

        public int CallsCount { get; set; }
    }
}
