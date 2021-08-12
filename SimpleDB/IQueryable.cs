using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SimpleDB
{
    public interface IQueryable<TEntity>
    {
        IQueryableSelect<TEntity> Select(Expression<Func<TEntity, object>> selectExpression = null);

        IQueryableSelect<TEntity> Where(Expression<Func<TEntity, bool>> whereExpression);

        IQueryableSelect<TEntity> OrderBy(Expression<Func<TEntity, object>> orderbyExpression, SortDirection direction);

        IQueryableSelect<TEntity> Skip(int value);

        IQueryableSelect<TEntity> Limit(int value);

        List<TEntity> ToList();

        int Update(Expression<Func<TEntity, TEntity>> updateExpression, Expression<Func<TEntity, bool>> whereExpression = null);

        int Delete(Expression<Func<TEntity, bool>> whereExpression = null);

        int Count();
    }
}
