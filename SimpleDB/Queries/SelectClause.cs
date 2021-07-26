using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal class SelectClause
    {
        public SelectClause(IEnumerable<SelectClauseItem> selectItems)
        {
            SelectItems = selectItems;
        }

        public IEnumerable<SelectClauseItem> SelectItems { get; }

        public abstract class SelectClauseItem { }

        public class PrimaryKey : SelectClauseItem
        {
        }

        public class Field : SelectClauseItem
        {
            public Field(byte number)
            {
                Number = number;
            }

            public byte Number { get; }
        }
    }
}
