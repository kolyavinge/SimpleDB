using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal class SelectQuery
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
    }
}
