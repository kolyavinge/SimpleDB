using System.Collections.Generic;
using SimpleDB.Core;

namespace SimpleDB.IndexedSearch
{
    internal class IndexValueConverter
    {
        private readonly IndexMeta _indexMeta;

        public IndexValueConverter(IndexMeta indexMeta)
        {
            _indexMeta = indexMeta;
        }

        public IEnumerable<FieldValue> GetFieldValues(IndexValue indexValue, IndexItem indexItem)
        {
            if (_indexMeta.IndexedFieldNumber != PrimaryKey.FieldNumber)
            {
                yield return new FieldValue(_indexMeta.IndexedFieldNumber, indexValue.IndexedFieldValue);
            }
            var includedFieldNumbers = _indexMeta.IncludedFieldNumbers ?? new byte[0];
            for (int includedFieldNumberIndex = 0; includedFieldNumberIndex < includedFieldNumbers.Length; includedFieldNumberIndex++)
            {
                var number = includedFieldNumbers[includedFieldNumberIndex];
                var value = indexItem.IncludedFields[includedFieldNumberIndex];
                yield return new FieldValue(number, value);
            }
        }
    }
}
