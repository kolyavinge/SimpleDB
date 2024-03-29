﻿using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Queries;

internal class UpdateClause
{
    public UpdateClause(IEnumerable<UpdateClauseItem> updateItems)
    {
        UpdateItems = updateItems;
    }

    public IEnumerable<UpdateClauseItem> UpdateItems { get; }

    public IEnumerable<byte> GetAllFieldNumbers()
    {
        return UpdateItems.OfType<Field>().Select(x => x.Number).Distinct();
    }

    public abstract class UpdateClauseItem { }

    public class Field : UpdateClauseItem
    {
        public Field(byte number, object? value)
        {
            Number = number;
            Value = value;
        }

        public byte Number { get; }

        public object? Value { get; }
    }
}
