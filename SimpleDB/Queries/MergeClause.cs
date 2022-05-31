using System.Collections.Generic;

namespace SimpleDB.Queries;

internal class MergeClause
{
    public MergeClause(IEnumerable<MergeClauseItem> mergeItems)
    {
        MergeItems = mergeItems;
    }

    public IEnumerable<MergeClauseItem> MergeItems { get; }

    internal class MergeClauseItem
    {
        public MergeClauseItem(byte fieldNumber)
        {
            FieldNumber = fieldNumber;
        }

        public byte FieldNumber { get; }
    }
}
