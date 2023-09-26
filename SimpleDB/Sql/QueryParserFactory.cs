using System;
using System.Linq;
using System.Reflection;

namespace SimpleDB.Sql;

internal class QueryParserFactory
{
    public QueryParser MakeParser(QueryType queryType)
    {
        return Assembly.GetExecutingAssembly()
            .GetTypes()
            .Where(t =>
                t.IsClass
                && !t.IsAbstract
                && t.GetCustomAttribute<QueryParserAttribute>() is not null
                && t.GetCustomAttribute<QueryParserAttribute>().QueryType == queryType)
            .Select(t => (QueryParser)Activator.CreateInstance(t))
            .First();
    }
}

[AttributeUsage(AttributeTargets.Class)]
internal class QueryParserAttribute : Attribute
{
    public QueryType QueryType { get; }

    public QueryParserAttribute(QueryType queryType)
    {
        QueryType = queryType;
    }
}
