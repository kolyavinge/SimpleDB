using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Sql;

[QueryParserAttribute(QueryType.Select)]
internal class SelectQueryParser : QueryParser
{
    enum State
    {
        Select,
        SelectClause,
        SelectNextField,
        SelectField,
        From,
        FromTable,
        Where,
        OrderBy,
        OrderByItem,
        OrderByDirection,
        OrderByDirectionAscDesc,
        OrderByEnd,
        Skip,
        SkipValue,
        Limit,
        LimitValue,
        End
    }

    class OrderByItem
    {
        public readonly string Field;
        public SortDirection Direction;

        public OrderByItem(string field, SortDirection direction)
        {
            Field = field;
            Direction = direction;
        }
    }

    public override AbstractQuery GetQuery(QueryContext context, List<Token> tokens)
    {
        var tokenIter = new TokenIterator(tokens);
        EntityMeta entityMeta;
        bool selectAll = false;
        var selectTokens = new List<Token>();
        SelectQuery? selectQuery = null;
        var orderByItems = new List<OrderByItem>();

        switch (State.Select)
        {
            case State.Select:
                if (tokenIter.Eof) break;
                else if (tokenIter.Current.Kind == TokenKind.SelectKeyword) { tokenIter.NextToken(); goto case State.SelectClause; }
                else throw new InvalidQueryException();
            case State.SelectClause:
                if (tokenIter.Current.Kind == TokenKind.Asterisk) { selectAll = true; tokenIter.NextToken(); goto case State.From; }
                else if (tokenIter.Current.Kind == TokenKind.Identificator) { selectTokens.Add(tokenIter.Current); tokenIter.NextToken(); goto case State.SelectNextField; }
                else throw new InvalidQueryException();
            case State.SelectNextField:
                if (tokenIter.Current.Kind == TokenKind.FromKeyword) { tokenIter.NextToken(); goto case State.FromTable; }
                else if (tokenIter.Current.Kind == TokenKind.Comma) { tokenIter.NextToken(); goto case State.SelectField; }
                else throw new InvalidQueryException();
            case State.SelectField:
                if (tokenIter.Current.Kind == TokenKind.Identificator) { selectTokens.Add(tokenIter.Current); tokenIter.NextToken(); goto case State.SelectNextField; }
                else throw new InvalidQueryException();
            case State.From:
                if (tokenIter.Current.Kind == TokenKind.FromKeyword) { tokenIter.NextToken(); goto case State.FromTable; }
                else throw new InvalidQueryException();
            case State.FromTable:
                if (tokenIter.Current.Kind == TokenKind.Identificator)
                {
                    if (!context.EntityMetaDictionary.ContainsKey(tokenIter.Current.Value)) throw new InvalidQueryException();
                    entityMeta = context.EntityMetaDictionary[tokenIter.Current.Value];
                    selectQuery = new SelectQuery(entityMeta.EntityName, new SelectClause(GetSelectClauseItems(entityMeta, selectTokens, selectAll)));
                    tokenIter.NextToken();
                    goto case State.Where;
                }
                else throw new InvalidQueryException();
            case State.Where:
                if (tokenIter.Eof) break;
                if (tokenIter.Current.Kind == TokenKind.WhereKeyword)
                {
                    var whereClauseParser = new WhereClauseParser(entityMeta, tokenIter);
                    selectQuery.WhereClause = whereClauseParser.GetClause();
                }
                goto case State.OrderBy;
            case State.OrderBy:
                if (tokenIter.Eof) break;
                else if (tokenIter.Current.Kind == TokenKind.OrderByKeyword)
                {
                    tokenIter.NextToken();
                    goto case State.OrderByItem;
                }
                goto case State.Skip;
            case State.OrderByItem:
                if (tokenIter.Current.Kind == TokenKind.Identificator)
                {
                    orderByItems.Add(new OrderByItem(tokenIter.Current.Value, SortDirection.Asc));
                    tokenIter.NextToken();
                    goto case State.OrderByDirection;
                }
                else throw new InvalidQueryException();
            case State.OrderByDirection:
                if (tokenIter.Current.Kind == TokenKind.Comma) { tokenIter.NextToken(); goto case State.OrderByItem; }
                else if (tokenIter.Current.Kind == TokenKind.AscKeyword) { tokenIter.NextToken(); goto case State.OrderByDirectionAscDesc; }
                else if (tokenIter.Current.Kind == TokenKind.DescKeyword) { orderByItems.Last().Direction = SortDirection.Desc; tokenIter.NextToken(); goto case State.OrderByDirectionAscDesc; }
                else if (tokenIter.Current.Kind == TokenKind.SkipKeyword) { goto case State.OrderByEnd; }
                else if (tokenIter.Current.Kind == TokenKind.LimitKeyword) { goto case State.OrderByEnd; }
                else goto case State.OrderByEnd;
            case State.OrderByDirectionAscDesc:
                if (tokenIter.Current.Kind == TokenKind.Comma) { tokenIter.NextToken(); goto case State.OrderByItem; }
                else if (tokenIter.Current.Kind == TokenKind.SkipKeyword) { goto case State.OrderByEnd; }
                else if (tokenIter.Current.Kind == TokenKind.LimitKeyword) { goto case State.OrderByEnd; }
                else goto case State.OrderByEnd;
            case State.OrderByEnd:
                selectQuery.OrderByClause = new OrderByClause(GetOrderByClauseItems(entityMeta, orderByItems));
                goto case State.Skip;
            case State.Skip:
                if (tokenIter.Eof) break;
                if (tokenIter.Current.Kind == TokenKind.SkipKeyword) { tokenIter.NextToken(); goto case State.SkipValue; }
                else goto case State.Limit;
            case State.SkipValue:
                if (tokenIter.Current.Kind == TokenKind.IntegerNumber)
                {
                    selectQuery.Skip = Int32.Parse(tokenIter.Current.Value);
                    tokenIter.NextToken();
                }
                else throw new InvalidQueryException();
                goto case State.Limit;
            case State.Limit:
                if (tokenIter.Eof) break;
                if (tokenIter.Current.Kind == TokenKind.LimitKeyword) { tokenIter.NextToken(); goto case State.LimitValue; }
                else goto case State.End;
            case State.LimitValue:
                if (tokenIter.Current.Kind == TokenKind.IntegerNumber)
                {
                    selectQuery.Limit = Int32.Parse(tokenIter.Current.Value);
                    tokenIter.NextToken();
                }
                else throw new InvalidQueryException();
                goto case State.End;
            case State.End:
                if (!tokenIter.Eof) throw new InvalidQueryException();
                break;
        }

        return selectQuery!;
    }

    private IEnumerable<SelectClause.SelectClauseItem> GetSelectClauseItems(EntityMeta entityMeta, IEnumerable<Token> selectTokens, bool selectAll)
    {
        if (selectAll)
        {
            yield return new SelectClause.PrimaryKey();
            foreach (var field in entityMeta.FieldMetaCollection)
            {
                yield return new SelectClause.Field(field.Number);
            }
        }
        else
        {
            foreach (var token in selectTokens)
            {
                if (token.Value.Equals(entityMeta.PrimaryKeyFieldMeta.Name))
                {
                    yield return new SelectClause.PrimaryKey();
                }
                else
                {
                    var fieldMeta = entityMeta.FieldMetaCollection.FirstOrDefault(x => token.Value.Equals(x.Name));
                    if (fieldMeta is null) throw new InvalidQueryException();
                    yield return new SelectClause.Field(fieldMeta.Number);
                }
            }
        }
    }

    private IEnumerable<OrderByClause.OrderByClauseItem> GetOrderByClauseItems(EntityMeta entityMeta, IEnumerable<OrderByItem> orderByItems)
    {
        foreach (var orderByItem in orderByItems)
        {
            if (orderByItem.Field.Equals(entityMeta.PrimaryKeyFieldMeta.Name))
            {
                yield return new OrderByClause.PrimaryKey(orderByItem.Direction);
            }
            else
            {
                var fieldMeta = entityMeta.FieldMetaCollection.FirstOrDefault(x => orderByItem.Field.Equals(x.Name));
                if (fieldMeta is null) throw new InvalidQueryException();
                yield return new OrderByClause.Field(fieldMeta.Number, orderByItem.Direction);
            }
        }
    }
}
