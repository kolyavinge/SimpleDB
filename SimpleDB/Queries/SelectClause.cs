using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Queries
{
    internal class SelectClause
    {
        public SelectClause(IEnumerable<SelectClauseItem> selectItems)
        {
            SelectItems = selectItems;
        }

        public IEnumerable<SelectClauseItem> SelectItems { get; }

        public IEnumerable<byte> GetAllFieldNumbers()
        {
            return SelectItems.Where(x => x is Field).Cast<Field>().Select(x => x.Number).Distinct();
        }

        public abstract class SelectClauseItem { }

        public class PrimaryKey : SelectClauseItem { }

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
