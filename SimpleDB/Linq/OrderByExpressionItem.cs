using System;
using System.Linq.Expressions;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class OrderByExpressionItem<TEntity>
    {
        public Expression<Func<TEntity, object>> Expression { get; set; }

        public SortDirection Direction { get; set; }
    }
}
