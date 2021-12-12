using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Linq
{
    internal class QueryableSelect<TEntity> : IQueryableSelect<TEntity>
    {
        private readonly SelectQueryExecutor _queryExecutor;
        private readonly Mapper<TEntity> _mapper;
        private Expression<Func<TEntity, object>> _selectExpression;
        private Expression<Func<TEntity, bool>> _whereExpression;
        private List<OrderByExpressionItem<TEntity>> _orderbyExpressionItems;
        private int? _skipValue;
        private int? _limitValue;

        public QueryableSelect(
            SelectQueryExecutor queryExecutor,
            Mapper<TEntity> mapper,
            Expression<Func<TEntity, object>> selectExpression)
        {
            _queryExecutor = queryExecutor;
            _mapper = mapper;
            _selectExpression = selectExpression;
            _orderbyExpressionItems = new List<OrderByExpressionItem<TEntity>>();
        }

        public IQueryableSelect<TEntity> Where(Expression<Func<TEntity, bool>> whereExpression)
        {
            if (_whereExpression != null) throw new QueryableSelectException("Where clause is already exist in the query");
            _whereExpression = whereExpression;
            return this;
        }

        public IQueryableSelect<TEntity> OrderBy(Expression<Func<TEntity, object>> orderbyExpression, SortDirection direction)
        {
            var item = new OrderByExpressionItem<TEntity> { Expression = orderbyExpression, Direction = direction };
            _orderbyExpressionItems.Add(item);
            return this;
        }

        public IQueryableSelect<TEntity> Skip(int value)
        {
            _skipValue = value;
            return this;
        }

        public IQueryableSelect<TEntity> Limit(int value)
        {
            _limitValue = value;
            return this;
        }

        public List<TEntity> ToList()
        {
            var queryBuilder = new SelectQueryBuilder<TEntity>(
                _mapper,
                _selectExpression,
                _whereExpression,
                _orderbyExpressionItems,
                _skipValue,
                _limitValue);
            var query = queryBuilder.BuildQueryForToList();
            var result = _queryExecutor.ExecuteQuery(query);
            var entities = _queryExecutor.MakeEntities(query, result, _mapper);

            return entities;
        }

        public int Count()
        {
            var queryBuilder = new SelectQueryBuilder<TEntity>(
                _mapper,
                _selectExpression,
                _whereExpression,
                _orderbyExpressionItems,
                _skipValue,
                _limitValue);
            var query = queryBuilder.BuildQueryForCount();
            var result = _queryExecutor.ExecuteQuery(query);

            return (int)result.Scalar;
        }
    }

    internal class QueryableSelectException : Exception
    {
        public QueryableSelectException(string message) : base(message)
        {
        }
    }
}
