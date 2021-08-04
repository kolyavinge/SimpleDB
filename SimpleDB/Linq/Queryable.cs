using System;
using System.Linq.Expressions;
using SimpleDB.Core;

namespace SimpleDB.Linq
{
    internal class Queryable<TEntity> : IQueryable<TEntity>
    {
        private readonly QueryExecutor<TEntity> _queryExecutor;
        private readonly Mapper<TEntity> _mapper;

        public Queryable(QueryExecutor<TEntity> queryExecutor, Mapper<TEntity> mapper)
        {
            _queryExecutor = queryExecutor;
            _mapper = mapper;
        }

        public IQueryableSelect<TEntity> Select(Expression<Func<TEntity, object>> selectExpression = null)
        {
            return new QueryableSelect<TEntity>(_queryExecutor, _mapper, selectExpression);
        }
    }
}
