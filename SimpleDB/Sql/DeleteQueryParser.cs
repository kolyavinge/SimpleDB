using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Sql
{
    [QueryParserAttribute(QueryType.Delete)]
    internal class DeleteQueryParser : QueryParser
    {
        enum State
        {
            Delete,
            DeleteTable,
            Where,
            End
        }

        public override AbstractQuery GetQuery(QueryContext context, List<Token> tokens)
        {
            var tokenIter = new TokenIterator(tokens);
            EntityMeta entityMeta;
            DeleteQuery deleteQuery;

            switch (State.Delete)
            {
                case State.Delete:
                    if (tokenIter.Current.Kind == TokenKind.DeleteKeyword) { tokenIter.NextToken(); goto case State.DeleteTable; }
                    else throw new InvalidQueryException();
                case State.DeleteTable:
                    if (tokenIter.Current.Kind == TokenKind.Identificator)
                    {
                        if (!context.EntityMetaDictionary.ContainsKey(tokenIter.Current.Value)) throw new InvalidQueryException();
                        entityMeta = context.EntityMetaDictionary[tokenIter.Current.Value];
                        deleteQuery = new DeleteQuery(entityMeta.EntityName);
                        tokenIter.NextToken();
                        goto case State.Where;
                    }
                    else throw new InvalidQueryException();
                case State.Where:
                    if (tokenIter.Current.Kind == TokenKind.WhereKeyword)
                    {
                        var whereClauseParser = new WhereClauseParser();
                        deleteQuery.WhereClause = whereClauseParser.GetClause(entityMeta, tokenIter);
                    }
                    goto case State.End;
                case State.End:
                    if (!tokenIter.Eof) throw new InvalidQueryException();
                    break;
            }

            return deleteQuery;
        }
    }
}
