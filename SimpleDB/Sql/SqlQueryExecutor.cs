using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.IndexedSearch;
using SimpleDB.Queries;
using SimpleDB.QueryExecutors;

namespace SimpleDB.Sql
{
    internal interface ISqlQueryExecutor
    {
        SqlQueryResult ExecuteQuery(QueryContext context, string sqlQuery);
    }

    internal class SqlQueryExecutor : ISqlQueryExecutor
    {
        private readonly IPrimaryKeyFileFactory _primaryKeyFileFactory;
        private readonly IDataFileFactory _dataFileFactory;
        private readonly IndexHolder _indexHolder;

        public SqlQueryExecutor(
            IPrimaryKeyFileFactory primaryKeyFileFactory,
            IDataFileFactory dataFileFactory,
            IndexHolder indexHolder)
        {
            _primaryKeyFileFactory = primaryKeyFileFactory;
            _dataFileFactory = dataFileFactory;
            _indexHolder = indexHolder;
        }

        public SqlQueryResult ExecuteQuery(QueryContext context, string sqlQuery)
        {
            var scanner = new Scanner(sqlQuery);
            var tokens = scanner.GetTokens().ToList();
            var queryType = GetQueryType(tokens);
            var factory = new QueryParserFactory();
            var parser = factory.MakeParser(queryType);
            var query = parser.GetQuery(context, tokens);
            var entityMeta = context.EntityMetaCollection.First(x => x.EntityName == query.EntityName);
            var primaryKeyFile = _primaryKeyFileFactory.MakeFromEntityName(entityMeta.EntityName, entityMeta.PrimaryKeyType);
            primaryKeyFile.BeginRead();
            var primaryKeys = primaryKeyFile.GetAllPrimaryKeys().ToDictionary(k => k.Value, v => v);
            primaryKeyFile.EndReadWrite();
            var dataFile = _dataFileFactory.MakeFromEntityName(entityMeta.EntityName, entityMeta.FieldMetaCollection);
            var result = ExecuteQuery(queryType, query, primaryKeyFile, primaryKeys, dataFile);

            return result;
        }

        private SqlQueryResult ExecuteQuery(
            QueryType queryType,
            AbstractQuery query,
            PrimaryKeyFile primaryKeyFile,
            IDictionary<object, PrimaryKey> primaryKeys,
            DataFile dataFile)
        {
            if (queryType == QueryType.Select)
            {
                var executor = new SelectQueryExecutor(dataFile, primaryKeys, _indexHolder);
                var result = executor.ExecuteQuery((SelectQuery)query);
                return new SqlQueryResult
                {
                    FieldValueCollections = result.FieldValueCollections,
                    Scalar = result.Scalar
                };
            }

            throw new Exception();
        }

        private QueryType GetQueryType(List<Token> tokens)
        {
            if (tokens.First().IsValueEquals("SELECT"))
            {
                return QueryType.Select;
            }

            throw new Exception();
        }
    }

    internal class SqlQueryResult
    {
        public List<FieldValueCollection> FieldValueCollections { get; set; }

        public object Scalar { get; set; }
    }
}
