using System;
using System.Linq.Expressions;

namespace SimpleDB
{
    public interface IQueryable<TEntity>
    {
        IQueryableSelect<TEntity> Select(Expression<Func<TEntity, object>> selectExpression);
    }
}
