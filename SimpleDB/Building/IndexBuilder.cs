using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;

namespace SimpleDB.Building;

internal abstract class IndexBuilder
{
    public Func<MapperHolder, IIndex>? BuildFunction { get; protected set; }
}

internal class IndexBuilder<TEntity> : IndexBuilder, IIndexBuilder<TEntity>
{
    private string? _name;
    private readonly List<Expression<Func<TEntity, object>>> _includeExpressions;
    private readonly IFileSystem _fileSystem;

    public IndexBuilder(IFileSystem fileSystem)
    {
        _includeExpressions = new List<Expression<Func<TEntity, object>>>();
        _fileSystem = fileSystem;
    }

    public IIndexBuilder<TEntity> Name(string name)
    {
        _name = name;
        return this;
    }

    public IIndexBuilder<TEntity> For<TField>(Expression<Func<TEntity, TField>> indexedFieldExpression) where TField : IComparable<TField>
    {
        BuildFunction = mapperHolder =>
        {
            var initializer = new IndexInitializer<TEntity>(
                mapperHolder, new PrimaryKeyFileFactory(_fileSystem), new DataFileFactory(_fileSystem), new IndexFileFactory(_fileSystem));

            return initializer.GetIndex(_name!, indexedFieldExpression, _includeExpressions);
        };

        return this;
    }

    public IIndexBuilder<TEntity> Include(Expression<Func<TEntity, object>> includeExpression)
    {
        _includeExpressions.Add(includeExpression);
        return this;
    }
}
