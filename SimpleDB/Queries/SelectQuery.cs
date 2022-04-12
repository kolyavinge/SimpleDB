using System.Collections.Generic;

namespace SimpleDB.Queries
{
    internal class SelectQuery : AbstractQuery
    {
        public SelectQuery(string entityName, SelectClause selectClause) : base(entityName)
        {
            SelectClause = selectClause;
        }

        public SelectClause SelectClause { get; }

        public WhereClause? WhereClause { get; set; }

        public OrderByClause? OrderByClause { get; set; }

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
