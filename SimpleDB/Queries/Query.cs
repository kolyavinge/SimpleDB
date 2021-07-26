using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Queries
{
    internal class Query
    {
        public Query(SelectClause selectClause, WhereClause whereClause)
        {
            SelectClause = selectClause;
            WhereClause = whereClause;
        }

        public SelectClause SelectClause { get; }

        public WhereClause WhereClause { get; }
    }
}
