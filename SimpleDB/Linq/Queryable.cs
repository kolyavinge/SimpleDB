using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class Queryable<TEntity> : IQueryable<TEntity>
    {
        private readonly Func<Query, List<TEntity>> _queryExecutorFunc;
        private readonly Mapper<TEntity> _mapper;

        public Queryable(Func<Query, List<TEntity>> queryExecutorFunc, Mapper<TEntity> mapper)
        {
            _queryExecutorFunc = queryExecutorFunc;
            _mapper = mapper;
        }

        public IQueryableSelect<TEntity> Select(Expression<Func<TEntity, object>> selectExpression)
        {
            return new QueryableSelect<TEntity>(_queryExecutorFunc, _mapper, selectExpression);
        }
    }
}
