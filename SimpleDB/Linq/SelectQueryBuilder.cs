﻿using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Linq;

internal class SelectQueryBuilder<TEntity>
{
    private readonly Mapper<TEntity> _mapper;
    private readonly Expression<Func<TEntity, object>>? _selectExpression;
    private readonly Expression<Func<TEntity, bool>>? _whereExpression;
    private readonly List<OrderByExpressionItem<TEntity>>? _orderbyExpressionItems;
    private readonly int? _skipValue;
    private readonly int? _limitValue;

    public SelectQueryBuilder(
        Mapper<TEntity> mapper,
        Expression<Func<TEntity, object>>? selectExpression,
        Expression<Func<TEntity, bool>>? whereExpression = null,
        List<OrderByExpressionItem<TEntity>>? orderbyExpressionItems = null,
        int? skipValue = null,
        int? limitValue = null)
    {
        _mapper = mapper;
        _selectExpression = selectExpression;
        _whereExpression = whereExpression;
        _orderbyExpressionItems = orderbyExpressionItems;
        _skipValue = skipValue;
        _limitValue = limitValue;
    }

    public SelectQuery BuildQueryForToList()
    {
        var selectClauseBuilder = new SelectClauseBuilder();
        var whereClauseBuilder = new WhereClauseBuilder();
        var orderbyClauseBuilder = new OrderByClauseBuilder();
        var query = new SelectQuery(_mapper.EntityName, selectClauseBuilder.Build(_mapper, _selectExpression));
        query.WhereClause = whereClauseBuilder.Build(_mapper, _whereExpression);
        query.OrderByClause = orderbyClauseBuilder.Build(_mapper, _orderbyExpressionItems);
        query.Skip = _skipValue;
        query.Limit = _limitValue;

        return query;
    }

    public SelectQuery BuildQueryForCount()
    {
        var selectClauseBuilder = new SelectClauseBuilder();
        var whereClauseBuilder = new WhereClauseBuilder();
        var query = new SelectQuery(_mapper.EntityName, selectClauseBuilder.BuildCountAggregate());
        query.WhereClause = whereClauseBuilder.Build(_mapper, _whereExpression);
        query.Skip = _skipValue;
        query.Limit = _limitValue;

        return query;
    }
}
