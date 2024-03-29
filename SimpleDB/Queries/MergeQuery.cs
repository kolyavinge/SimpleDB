﻿using System.Collections.Generic;

namespace SimpleDB.Queries;

internal class MergeQuery<TEntity> : AbstractQuery
{
    public MergeQuery(string entityName, MergeClause mergeClause, IEnumerable<TEntity> entities) : base(entityName)
    {
        MergeClause = mergeClause;
        Entities = entities;
    }

    public MergeClause MergeClause { get; }
    public IEnumerable<TEntity> Entities { get; }
}
