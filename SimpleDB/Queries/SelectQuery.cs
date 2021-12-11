using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SimpleDB.Queries
{
    internal class SelectQuery : AbstractQuery
    {
        public SelectQuery(SelectClause selectClause)
        {
            SelectClause = selectClause;
        }

        public SelectClause SelectClause { get; }

        public WhereClause WhereClause { get; set; }

        public OrderByClause OrderByClause { get; set; }

        public int? Skip { get; set; }

        public int? Limit { get; set; }

        public IEnumerable<byte> GetAllFieldNumbers()
        {
            foreach (var x in SelectClause.GetAllFieldNumbers()) yield return x;

            if (WhereClause != null)
            {
                foreach (var x in WhereClause.GetAllFieldNumbers()) yield return x;
            }

            if (OrderByClause != null)
            {
                foreach (var x in OrderByClause.GetAllFieldNumbers()) yield return x;
            }
        }
    }
}
