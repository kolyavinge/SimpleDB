using System;
using System.Linq.Expressions;

namespace SimpleDB.Linq;

internal class OrderByExpressionItem<TEntity>
{
    public Expression<Func<TEntity, object>> Expression { get; }

    public SortDirection Direction { get; }

    public OrderByExpressionItem(Expression<Func<TEntity, object>> expression, SortDirection direction)
    {
        Expression = expression;
        Direction = direction;
    }
}
