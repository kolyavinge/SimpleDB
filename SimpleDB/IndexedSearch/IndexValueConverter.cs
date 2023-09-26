using System.Collections.Generic;
using SimpleDB.Core;

namespace SimpleDB.IndexedSearch;

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

        if (_indexMeta.IncludedFieldNumbers is not null)
        {
            for (int includedFieldNumberIndex = 0; includedFieldNumberIndex < _indexMeta.IncludedFieldNumbers.Length; includedFieldNumberIndex++)
            {
                var number = _indexMeta.IncludedFieldNumbers[includedFieldNumberIndex];
                var value = indexItem.IncludedFields![includedFieldNumberIndex];
                yield return new FieldValue(number, value);
            }
        }
    }
}
