using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Linq
{
    internal class Queryable<TEntity> : IQueryable<TEntity>
    {
        private readonly QueryExecutorFactory _queryExecutorFactory;
        private readonly Mapper<TEntity> _mapper;

        public Queryable(QueryExecutorFactory queryExecutorFactory, Mapper<TEntity> mapper)
        {
            _queryExecutorFactory = queryExecutorFactory;
            _mapper = mapper;
        }

        public IQueryableSelect<TEntity> Select(Expression<Func<TEntity, object>>? selectExpression = null)
        {
            return MakeQueryableSelect(selectExpression);
        }

        public IQueryableSelect<TEntity> Where(Expression<Func<TEntity, bool>> whereExpression)
        {
            return MakeQueryableSelect().Where(whereExpression);
        }

        public IQueryableSelect<TEntity> OrderBy(Expression<Func<TEntity, object>> orderbyExpression, SortDirection direction)
        {
            return MakeQueryableSelect().OrderBy(orderbyExpression, direction);
        }

        public IQueryableSelect<TEntity> Skip(int value)
        {
            return MakeQueryableSelect().Skip(value);
        }

        public IQueryableSelect<TEntity> Limit(int value)
        {
            return MakeQueryableSelect().Limit(value);
        }

        public List<TEntity> ToList()
        {
            return MakeQueryableSelect().ToList();
        }

        public int Count()
        {
            return MakeQueryableSelect().Count();
        }

        public int Update(Expression<Func<TEntity, TEntity>> updateExpression, Expression<Func<TEntity, bool>>? whereExpression = null)
        {
            var queryBuilder = new UpdateQueryBuilder<TEntity>(
                _mapper,
                updateExpression,
                whereExpression);
            var query = queryBuilder.BuildQuery();
            var queryExecutor = _queryExecutorFactory.MakeUpdateQueryExecutor();
            var result = queryExecutor.ExecuteQuery(query);

            return result;
        }

        public int Delete(Expression<Func<TEntity, bool>>? whereExpression = null)
        {
            var queryBuilder = new DeleteQueryBuilder<TEntity>(
                _mapper,
                whereExpression);
            var query = queryBuilder.BuildQuery();
            var queryExecutor = _queryExecutorFactory.MakeDeleteQueryExecutor();
            var result = queryExecutor.ExecuteQuery(query);

            return result;
        }

        public IMergeQueryResult<TEntity> Merge(Expression<Func<TEntity, object>> mergeFieldsExpression, IEnumerable<TEntity> entities)
        {
            var queryBuilder = new MergeQueryBuilder<TEntity>(
               _mapper,
               mergeFieldsExpression,
               entities);
            var query = queryBuilder.BuildQuery();
            var queryExecutor = _queryExecutorFactory.MakeMergeQueryExecutor(_mapper);
            var result = queryExecutor.ExecuteQuery(query);

            return result;
        }

        private IQueryableSelect<TEntity> MakeQueryableSelect(Expression<Func<TEntity, object>>? selectExpression = null)
        {
            var queryExecutor = _queryExecutorFactory.MakeSelectQueryExecutor();
            return new QueryableSelect<TEntity>(queryExecutor, _mapper, selectExpression);
        }
    }
}
