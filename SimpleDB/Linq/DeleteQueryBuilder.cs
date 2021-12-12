using System;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq
{
    internal class DeleteQueryBuilder<TEntity>
    {
        private readonly Mapper<TEntity> _mapper;
        private readonly Expression<Func<TEntity, bool>> _whereExpression;

        public DeleteQueryBuilder(
            Mapper<TEntity> mapper,
            Expression<Func<TEntity, bool>> whereExpression = null)
        {
            _mapper = mapper;
            _whereExpression = whereExpression;
        }

        public DeleteQuery BuildQuery()
        {
            var whereClauseBuilder = new WhereClauseBuilder();
            var query = new DeleteQuery(_mapper.EntityName);
            query.WhereClause = whereClauseBuilder.Build(_mapper, _whereExpression);

            return query;
        }
    }
}
