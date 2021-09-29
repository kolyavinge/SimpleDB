﻿using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Utils.EnumerableExtension;

namespace SimpleDB.QueryExecutors
{
    internal interface IFieldValueReader
    {
        void ReadFieldValues(IEnumerable<FieldValueCollection> fieldValueCollections, IEnumerable<byte> fieldNumbers);
    }

    internal class FieldValueReader : IFieldValueReader
    {
        private readonly DataFile _dataFile;

        public FieldValueReader(DataFile dataFile)
        {
            _dataFile = dataFile;
        }

        public void ReadFieldValues(IEnumerable<FieldValueCollection> fieldValueCollections, IEnumerable<byte> fieldNumbers)
        {
            var remainingFieldNumbers = new HashSet<byte>();
            foreach (var fieldValueCollection in fieldValueCollections.OrderBy(x => x.PrimaryKey.StartDataFileOffset))
            {
                remainingFieldNumbers.Clear();
                remainingFieldNumbers.AddRange(fieldNumbers);
                remainingFieldNumbers.RemoveRange(fieldValueCollection.Select(x => x.Number));
                if (remainingFieldNumbers.Any())
                {
                    var primaryKey = fieldValueCollection.PrimaryKey;
                    _dataFile.ReadFields(primaryKey.StartDataFileOffset, primaryKey.EndDataFileOffset, remainingFieldNumbers, fieldValueCollection);
                }
            }
        }
    }
}
