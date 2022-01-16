using System;
using System.Collections.Generic;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Sql
{
    internal abstract class QueryParser
    {
        public abstract AbstractQuery GetQuery(QueryContext context, List<Token> tokens);
    }

    internal class InvalidQueryException : Exception
    {
        public InvalidQueryException() { }
    }
}
