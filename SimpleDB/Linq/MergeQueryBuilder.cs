using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq;

internal class MergeQueryBuilder<TEntity>
{
    private readonly Mapper<TEntity> _mapper;
    private readonly Expression<Func<TEntity, object>> _mergeFieldsExpression;
    private readonly IEnumerable<TEntity> _entities;

    public MergeQueryBuilder(
        Mapper<TEntity> mapper,
        Expression<Func<TEntity, object>> mergeFieldsExpression,
        IEnumerable<TEntity> entities)
    {
        _mapper = mapper;
        _mergeFieldsExpression = mergeFieldsExpression;
        _entities = entities;
    }

    public MergeQuery<TEntity> BuildQuery()
    {
        var mergeClauseBuilder = new MergeClauseBuilder();
        var mergeClause = mergeClauseBuilder.Build(_mapper, _mergeFieldsExpression);
        var query = new MergeQuery<TEntity>(_mapper.EntityName, mergeClause, _entities);

        return query;
    }
}
