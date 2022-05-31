using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using SimpleDB.Building;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Infrastructure;

namespace SimpleDB;

public sealed class DBEngineBuilder
{
    private readonly List<MapperBuilder> _mapperBuilders = new();
    private readonly List<IndexBuilder> _indexBuilders = new();
    private string? _databaseFilePath;
    private IFileSystem? _fileSystem;
    internal CollectionFactory? _collectionFactory;

    public static DBEngineBuilder Make()
    {
        return new DBEngineBuilder();
    }

    internal DBEngineBuilder() { }

    public void DatabaseFilePath(string path)
    {
        _databaseFilePath = path;
        _fileSystem = new FileSystem(_databaseFilePath);
        _collectionFactory = new CollectionFactory(_fileSystem);
    }

    public IMapperBuilder<TEntity> Map<TEntity>()
    {
        var mapperBuilder = new MapperBuilder<TEntity>();
        _mapperBuilders.Add(mapperBuilder);
        return mapperBuilder;
    }

    public IIndexBuilder<TEntity> Index<TEntity>()
    {
        var indexBuilder = new IndexBuilder<TEntity>(_fileSystem!);
        _indexBuilders.Add(indexBuilder);
        return indexBuilder;
    }

    public IDBEngine BuildEngine()
    {
        var mappers = _mapperBuilders.Select(x => x.Build()).ToList();
        var mapperHolder = new MapperHolder(mappers);
        var fileBuilder = new FileBuilder(_fileSystem!);
        fileBuilder.CreateNewFiles(mappers);
        var indexes = _indexBuilders.Select(x => x.BuildFunction!(mapperHolder)).ToList();
        var indexHolder = new IndexHolder(indexes);
        var indexUpdater = new IndexUpdater(indexes, new IndexFileFactory(_fileSystem!));

        return new DBEngine(_collectionFactory!, mapperHolder, indexHolder, indexUpdater);
    }
}

public interface IMapperBuilder<TEntity>
{
    IMapperBuilder<TEntity> PrimaryKey(Expression<Func<TEntity, object>> primaryKeyExpression);

    IMapperBuilder<TEntity> Field(byte number, Expression<Func<TEntity, object>> fieldExpression, FieldSettings settings = default);

    IMapperBuilder<TEntity> MakeFunction(Func<TEntity> func);

    IMapperBuilder<TEntity> PrimaryKeySetFunction(PrimaryKeySetFunctionDelegate<TEntity> func);

    IMapperBuilder<TEntity> FieldSetFunction(FieldSetFunctionDelegate<TEntity> func);
}

public delegate void PrimaryKeySetFunctionDelegate<in TEntity>(object primaryKeyValue, TEntity entity);

public delegate void FieldSetFunctionDelegate<in TEntity>(byte fieldNumber, object? fieldValue, TEntity entity);

public interface IIndexBuilder<TEntity>
{
    IIndexBuilder<TEntity> Name(string indexName);

    IIndexBuilder<TEntity> For<TField>(Expression<Func<TEntity, TField>> forExpression) where TField : IComparable<TField>;

    IIndexBuilder<TEntity> Include(Expression<Func<TEntity, object>> includeExpression);
}
