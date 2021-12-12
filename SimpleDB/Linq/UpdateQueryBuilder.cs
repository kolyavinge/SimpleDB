using System;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class UpdateQueryBuilder<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly Expression<Func<TEntity, TEntity>> _updateExpression;
        private readonly Expression<Func<TEntity, bool>> _whereExpression;

        public UpdateQueryBuilder(
            Mapper<TEntity> mapper,
            Expression<Func<TEntity, TEntity>> updateExpression,
            Expression<Func<TEntity, bool>> whereExpression = null)
        {
            _mapper = mapper;
            _updateExpression = updateExpression;
            _whereExpression = whereExpression;
        }

        public UpdateQuery BuildQuery()
        {
            var updateClauseBuilder = new UpdateClauseBuilder();
            var whereClauseBuilder = new WhereClauseBuilder();
            var query = new UpdateQuery(_mapper.EntityName, updateClauseBuilder.Build(_mapper, _updateExpression));
            query.WhereClause = whereClauseBuilder.Build(_mapper, _whereExpression);

            return query;
        }
    }
}
