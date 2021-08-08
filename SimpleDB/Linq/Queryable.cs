using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;

namespace SimpleDB.Linq
{
    internal class Queryable<TEntity> : IQueryable<TEntity>
    {
        private readonly QueryExecutor<TEntity> _queryExecutor;
        private readonly Mapper<TEntity> _mapper;
        private QueryableSelect<TEntity> _queryableSelect;

        public Queryable(QueryExecutor<TEntity> queryExecutor, Mapper<TEntity> mapper)
        {
            _queryExecutor = queryExecutor;
            _mapper = mapper;
        }

        public IQueryableSelect<TEntity> Select(Expression<Func<TEntity, object>> selectExpression = null)
        {
            return _queryableSelect ?? MakeQueryableSelect(selectExpression);
        }

        public IQueryableSelect<TEntity> Where(Expression<Func<TEntity, bool>> whereExpression)
        {
            return (_queryableSelect ?? MakeQueryableSelect()).Where(whereExpression);
        }

        public IQueryableSelect<TEntity> OrderBy(Expression<Func<TEntity, object>> orderbyExpression, SortDirection direction)
        {
            return (_queryableSelect ?? MakeQueryableSelect()).OrderBy(orderbyExpression, direction);
        }

        public IQueryableSelect<TEntity> Skip(int value)
        {
            return (_queryableSelect ?? MakeQueryableSelect()).Skip(value);
        }

        public IQueryableSelect<TEntity> Limit(int value)
        {
            return (_queryableSelect ?? MakeQueryableSelect()).Limit(value);
        }

        public List<TEntity> ToList()
        {
            return (_queryableSelect ?? MakeQueryableSelect()).ToList();
        }

        public int Count()
        {
            return (_queryableSelect ?? MakeQueryableSelect()).Count();
        }

        private IQueryableSelect<TEntity> MakeQueryableSelect(Expression<Func<TEntity, object>> selectExpression = null)
        {
            return new QueryableSelect<TEntity>(_queryExecutor, _mapper, selectExpression);
        }
    }
}
