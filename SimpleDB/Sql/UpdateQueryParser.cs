using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Sql;

[QueryParserAttribute(QueryType.Update)]
internal class UpdateQueryParser : QueryParser
{
    enum State
    {
        Update,
        UpdateTable,
        Set,
        SetField,
        SetNextField,
        SetValue,
        SetEnd,
        Where,
        End
    }

    class UpdateItem
    {
        public string? FieldName;
        public object? Value;
    }

    public override AbstractQuery GetQuery(QueryContext context, List<Token> tokens)
    {
        var tokenIter = new TokenIterator(tokens);
        EntityMeta entityMeta;
        UpdateQuery updateQuery;
        var updateItems = new List<UpdateItem>();

        switch (State.Update)
        {
            case State.Update:
                if (tokenIter.Current.Kind == TokenKind.UpdateKeyword) { tokenIter.NextToken(); goto case State.UpdateTable; }
                else throw new InvalidQueryException();
            case State.UpdateTable:
                if (tokenIter.Current.Kind == TokenKind.Identificator)
                {
                    if (!context.EntityMetaDictionary.ContainsKey(tokenIter.Current.Value)) throw new InvalidQueryException();
                    entityMeta = context.EntityMetaDictionary[tokenIter.Current.Value];
                    tokenIter.NextToken();
                    goto case State.Set;
                }
                else throw new InvalidQueryException();
            case State.Set:
                if (tokenIter.Current.Kind == TokenKind.SetKeyword) { tokenIter.NextToken(); goto case State.SetField; }
                else throw new InvalidQueryException();
            case State.SetField:
                if (tokenIter.Current.Kind == TokenKind.Identificator)
                {
                    updateItems.Add(new UpdateItem { FieldName = tokenIter.Current.Value });
                    tokenIter.NextToken();
                }
                else throw new InvalidQueryException();
                if (tokenIter.Current.Kind == TokenKind.EqualsOperation) { tokenIter.NextToken(); goto case State.SetValue; }
                else throw new InvalidQueryException();
            case State.SetValue:
                if (tokenIter.Current.Kind == TokenKind.IntegerNumber)
                {
                    updateItems.Last().Value = Int32.Parse(tokenIter.Current.Value);
                    tokenIter.NextToken();
                    goto case State.SetNextField;
                }
                else if (tokenIter.Current.Kind == TokenKind.FloatNumber)
                {
                    updateItems.Last().Value = Double.Parse(tokenIter.Current.Value, new NumberFormatInfo { NumberDecimalSeparator = "." });
                    tokenIter.NextToken();
                    goto case State.SetNextField;
                }
                else if (tokenIter.Current.Kind == TokenKind.String)
                {
                    updateItems.Last().Value = tokenIter.Current.Value;
                    tokenIter.NextToken();
                    goto case State.SetNextField;
                }
                else throw new InvalidQueryException();
            case State.SetNextField:
                if (tokenIter.Current.Kind == TokenKind.Comma) { tokenIter.NextToken(); goto case State.SetField; }
                else goto case State.SetEnd;
            case State.SetEnd:
                updateQuery = new UpdateQuery(entityMeta.EntityName, new UpdateClause(GetUpdateClauseItems(entityMeta, updateItems)));
                goto case State.Where;
            case State.Where:
                if (tokenIter.Current.Kind == TokenKind.WhereKeyword)
                {
                    var whereClauseParser = new WhereClauseParser(entityMeta, tokenIter);
                    updateQuery.WhereClause = whereClauseParser.GetClause();
                }
                goto case State.End;
            case State.End:
                if (!tokenIter.Eof) throw new InvalidQueryException();
                break;
        }

        return updateQuery;
    }

    private IEnumerable<UpdateClause.UpdateClauseItem> GetUpdateClauseItems(EntityMeta entityMeta, IEnumerable<UpdateItem> updateItems)
    {
        foreach (var item in updateItems)
        {
            var fieldMeta = entityMeta.FieldMetaCollection.FirstOrDefault(x => x.Name == item.FieldName);
            if (fieldMeta is null) throw new InvalidQueryException();
            var convertedValue = Convert.ChangeType(item.Value, fieldMeta.Type);
            yield return new UpdateClause.Field(fieldMeta.Number, convertedValue);
        }
    }
}
