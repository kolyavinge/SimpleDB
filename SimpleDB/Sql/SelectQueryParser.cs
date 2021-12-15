using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Sql
{
    [QueryParserAttribute(QueryType.Select)]
    internal class SelectQueryParser : QueryParser
    {
        public override AbstractQuery GetQuery(QueryContext context, List<Token> tokens)
        {
            var selectClauseItems = new List<SelectClause.SelectClauseItem>();
            int tokenIndex = 1;
            EntityMeta entityMeta = null;

            switch (1)
            {
                case 1:
                    if (tokenIndex == tokens.Count) break;
                    else if (tokens[tokenIndex].IsValueEquals("FROM")) { tokenIndex++; goto case 2; }
                    else { tokenIndex++; goto case 1; }
                case 2:
                    if (tokenIndex == tokens.Count) break;
                    else
                    {
                        var entityName = tokens[tokenIndex].Value;
                        entityMeta = context.EntityMetaCollection.First(x => x.EntityName == entityName);
                        tokenIndex = 1;
                        goto case 3;
                    }
                case 3:
                    if (tokenIndex == tokens.Count) break;
                    else if (tokens[tokenIndex].IsValueEquals("*"))
                    {
                        selectClauseItems.Add(new SelectClause.PrimaryKey());
                        selectClauseItems.AddRange(entityMeta.FieldMetaCollection.Select(x => new SelectClause.Field(x.Number)));
                        tokenIndex++;
                        goto case 3;
                    }
                    else if (tokens[tokenIndex].IsValueEquals("FROM")) { tokenIndex += 2; goto case 3; }
                    else
                    {
                        if (tokens[tokenIndex].IsValueEquals(entityMeta.PrimaryKeyName))
                        {
                            selectClauseItems.Add(new SelectClause.PrimaryKey());
                        }
                        else
                        {
                            var fieldMeta = entityMeta.FieldMetaCollection.First(x => tokens[tokenIndex].IsValueEquals(x.Name));
                            selectClauseItems.Add(new SelectClause.Field(fieldMeta.Number));
                        }
                        tokenIndex++;
                        goto case 3;
                    }
            }

            return new SelectQuery(entityMeta.EntityName, new SelectClause(selectClauseItems));
        }
    }
}
