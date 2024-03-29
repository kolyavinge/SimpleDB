﻿using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Sql;

internal interface ISqlQueryExecutor
{
    SqlQueryResult ExecuteQuery(string sqlQuery);
}

internal class SqlQueryExecutor : ISqlQueryExecutor
{
    private readonly Dictionary<string, EntityMeta> _entityMetaDictionary;
    private readonly IPrimaryKeyFileFactory _primaryKeyFileFactory;
    private readonly IDataFileFactory _dataFileFactory;
    private readonly IndexHolder _indexHolder;
    private readonly IIndexUpdater _indexUpdater;

    public SqlQueryExecutor(
        Dictionary<string, EntityMeta> entityMetaDictionary,
        IPrimaryKeyFileFactory primaryKeyFileFactory,
        IDataFileFactory dataFileFactory,
        IndexHolder indexHolder,
        IIndexUpdater indexUpdater)
    {
        _entityMetaDictionary = entityMetaDictionary;
        _primaryKeyFileFactory = primaryKeyFileFactory;
        _dataFileFactory = dataFileFactory;
        _indexHolder = indexHolder;
        _indexUpdater = indexUpdater;
    }

    public SqlQueryResult ExecuteQuery(string sqlQuery)
    {
        var scanner = new Scanner(sqlQuery);
        var tokens = scanner.GetTokens().ToList();
        var queryType = GetQueryType(tokens);
        var factory = new QueryParserFactory();
        var parser = factory.MakeParser(queryType);
        var context = new QueryContext(_entityMetaDictionary);
        var query = parser.GetQuery(context, tokens);
        var entityMeta = _entityMetaDictionary[query.EntityName];
        var primaryKeyFile = _primaryKeyFileFactory.MakeFromEntityName(entityMeta.EntityName, entityMeta.PrimaryKeyFieldMeta.Type);
        primaryKeyFile.BeginRead();
        var primaryKeys = primaryKeyFile.GetAllPrimaryKeys().Where(x => !x.IsDeleted).ToDictionary(k => k.Value, v => v);
        primaryKeyFile.EndReadWrite();
        var dataFile = _dataFileFactory.MakeFromEntityName(entityMeta.EntityName, entityMeta.FieldMetaCollection);
        var result = ExecuteQuery(queryType, entityMeta, query, primaryKeyFile, primaryKeys, dataFile);

        return result;
    }

    private SqlQueryResult ExecuteQuery(
        QueryType queryType,
        EntityMeta entityMeta,
        AbstractQuery query,
        PrimaryKeyFile primaryKeyFile,
        IDictionary<object, PrimaryKey> primaryKeys,
        DataFile dataFile)
    {
        if (queryType == QueryType.Select)
        {
            var executor = new SelectQueryExecutor(dataFile, primaryKeys, new FieldValueReader(dataFile), _indexHolder);
            var result = executor.ExecuteQuery((SelectQuery)query);
            return new SqlQueryResult(query.EntityName)
            {
                FieldValueCollections = result.FieldValueCollections,
                Scalar = result.Scalar
            };
        }
        if (queryType == QueryType.Update)
        {
            var executor = new UpdateQueryExecutor(entityMeta, primaryKeyFile, dataFile, primaryKeys, new FieldValueReader(dataFile), _indexHolder, _indexUpdater);
            var result = executor.ExecuteQuery((UpdateQuery)query);
            return new SqlQueryResult(query.EntityName)
            {
                Scalar = result
            };
        }
        if (queryType == QueryType.Delete)
        {
            var executor = new DeleteQueryExecutor(entityMeta, primaryKeyFile, dataFile, primaryKeys, new FieldValueReader(dataFile), _indexHolder, _indexUpdater);
            var result = executor.ExecuteQuery((DeleteQuery)query);
            return new SqlQueryResult(query.EntityName)
            {
                Scalar = result
            };
        }

        throw new InvalidQueryException();
    }

    private QueryType GetQueryType(List<Token> tokens)
    {
        var tokenKind = tokens.First().Kind;
        if (tokenKind == TokenKind.SelectKeyword) return QueryType.Select;
        if (tokenKind == TokenKind.UpdateKeyword) return QueryType.Update;
        if (tokenKind == TokenKind.DeleteKeyword) return QueryType.Delete;
        throw new InvalidQueryException();
    }
}

internal class SqlQueryResult
{
    public string EntityName { get; }

    public List<FieldValueCollection>? FieldValueCollections { get; set; }

    public object? Scalar { get; set; }

    public SqlQueryResult(string entityName)
    {
        EntityName = entityName;
    }
}
