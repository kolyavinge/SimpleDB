using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal class Query
    {
        public Query(SelectClause selectClause)
        {
            SelectClause = selectClause;
        }

        public SelectClause SelectClause { get; }

        public WhereClause WhereClause { get; set; }

        public OrderByClause OrderByClause { get; set; }

        public uint? Skip { get; set; }

        public uint? Limit { get; set; }
    }
}
