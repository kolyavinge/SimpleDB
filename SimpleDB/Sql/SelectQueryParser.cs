using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Sql
{
    [QueryParserAttribute(QueryType.Select)]
    internal class SelectQueryParser : QueryParser
    {
        enum State
        {
            Select,
            SelectClause,
            SelectFields,
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
            public string Field;
            public SortDirection Direction;
        }

        private TokenIterator _tokenIter;

        public override AbstractQuery GetQuery(QueryContext context, List<Token> tokens)
        {
            _tokenIter = new TokenIterator(tokens);
            EntityMeta entityMeta = null;
            bool selectAll = false;
            var selectTokens = new List<Token>();
            SelectQuery selectQuery = null;
            var orderByItems = new List<OrderByItem>();

            switch (State.Select)
            {
                case State.Select:
                    if (_tokenIter.Eof) break;
                    else if (_tokenIter.Current.Kind == TokenKind.SelectKeyword) { _tokenIter.NextToken(); goto case State.SelectClause; }
                    else throw new InvalidQueryException();
                case State.SelectClause:
                    if (_tokenIter.Current.Kind == TokenKind.Asterisk) { selectAll = true; _tokenIter.NextToken(); goto case State.From; }
                    else if (_tokenIter.Current.Kind == TokenKind.Identificator) { selectTokens.Add(_tokenIter.Current); _tokenIter.NextToken(); goto case State.SelectFields; }
                    else throw new InvalidQueryException();
                case State.SelectFields:
                    if (_tokenIter.Current.Kind == TokenKind.FromKeyword) { _tokenIter.NextToken(); goto case State.FromTable; }
                    else if (_tokenIter.Current.Kind == TokenKind.Comma) { _tokenIter.NextToken(); goto case State.SelectField; }
                    else throw new InvalidQueryException();
                case State.SelectField:
                    if (_tokenIter.Current.Kind == TokenKind.Identificator) { selectTokens.Add(_tokenIter.Current); _tokenIter.NextToken(); goto case State.SelectFields; }
                    else throw new InvalidQueryException();
                case State.From:
                    if (_tokenIter.Current.Kind == TokenKind.FromKeyword) { _tokenIter.NextToken(); goto case State.FromTable; }
                    else throw new InvalidQueryException();
                case State.FromTable:
                    if (_tokenIter.Current.Kind == TokenKind.Identificator)
                    {
                        entityMeta = context.EntityMetaCollection.FirstOrDefault(x => x.EntityName == _tokenIter.Current.Value);
                        if (entityMeta == null) throw new InvalidQueryException();
                        selectQuery = new SelectQuery(entityMeta.EntityName, new SelectClause(GetSelectClauseItems(entityMeta, selectTokens, selectAll)));
                        _tokenIter.NextToken();
                        goto case State.Where;
                    }
                    else throw new InvalidQueryException();
                case State.Where:
                    if (_tokenIter.Eof) break;
                    if (_tokenIter.Current.Kind == TokenKind.WhereKeyword)
                    {
                        var whereClauseParser = new WhereClauseParser();
                        selectQuery.WhereClause = whereClauseParser.GetClause(entityMeta, _tokenIter);
                    }
                    goto case State.OrderBy;
                case State.OrderBy:
                    if (_tokenIter.Eof) break;
                    else if (_tokenIter.Current.Kind == TokenKind.OrderByKeyword)
                    {
                        _tokenIter.NextToken();
                        goto case State.OrderByItem;
                    }
                    goto case State.Skip;
                case State.OrderByItem:
                    if (_tokenIter.Current.Kind == TokenKind.Identificator)
                    {
                        orderByItems.Add(new OrderByItem { Field = _tokenIter.Current.Value, Direction = SortDirection.Asc });
                        _tokenIter.NextToken();
                        goto case State.OrderByDirection;
                    }
                    else throw new InvalidQueryException();
                case State.OrderByDirection:
                    if (_tokenIter.Current.Kind == TokenKind.Comma) { _tokenIter.NextToken(); goto case State.OrderByItem; }
                    else if (_tokenIter.Current.Kind == TokenKind.AscKeyword) { _tokenIter.NextToken(); goto case State.OrderByDirectionAscDesc; }
                    else if (_tokenIter.Current.Kind == TokenKind.DescKeyword) { orderByItems.Last().Direction = SortDirection.Desc; _tokenIter.NextToken(); goto case State.OrderByDirectionAscDesc; }
                    else if (_tokenIter.Current.Kind == TokenKind.SkipKeyword) { goto case State.OrderByEnd; }
                    else if (_tokenIter.Current.Kind == TokenKind.LimitKeyword) { goto case State.OrderByEnd; }
                    else goto case State.OrderByEnd;
                case State.OrderByDirectionAscDesc:
                    if (_tokenIter.Current.Kind == TokenKind.Comma) { _tokenIter.NextToken(); goto case State.OrderByItem; }
                    else if (_tokenIter.Current.Kind == TokenKind.SkipKeyword) { goto case State.OrderByEnd; }
                    else if (_tokenIter.Current.Kind == TokenKind.LimitKeyword) { goto case State.OrderByEnd; }
                    else goto case State.OrderByEnd;
                case State.OrderByEnd:
                    selectQuery.OrderByClause = new OrderByClause(GetOrderByClauseItems(entityMeta, orderByItems));
                    goto case State.Skip;
                case State.Skip:
                    if (_tokenIter.Eof) break;
                    if (_tokenIter.Current.Kind == TokenKind.SkipKeyword) { _tokenIter.NextToken(); goto case State.SkipValue; }
                    else goto case State.Limit;
                case State.SkipValue:
                    if (_tokenIter.Current.Kind == TokenKind.IntegerNumber)
                    {
                        selectQuery.Skip = Int32.Parse(_tokenIter.Current.Value);
                        _tokenIter.NextToken();
                    }
                    else throw new InvalidQueryException();
                    goto case State.Limit;
                case State.Limit:
                    if (_tokenIter.Eof) break;
                    if (_tokenIter.Current.Kind == TokenKind.LimitKeyword) { _tokenIter.NextToken(); goto case State.LimitValue; }
                    else goto case State.End;
                case State.LimitValue:
                    if (_tokenIter.Current.Kind == TokenKind.IntegerNumber)
                    {
                        selectQuery.Limit = Int32.Parse(_tokenIter.Current.Value);
                        _tokenIter.NextToken();
                    }
                    else throw new InvalidQueryException();
                    goto case State.End;
                case State.End:
                    if (!_tokenIter.Eof) throw new InvalidQueryException();
                    break;
            }

            return selectQuery;
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
                    if (token.Value.Equals(entityMeta.PrimaryKeyName))
                    {
                        yield return new SelectClause.PrimaryKey();
                    }
                    else
                    {
                        var fieldMeta = entityMeta.FieldMetaCollection.First(x => token.Value.Equals(x.Name));
                        yield return new SelectClause.Field(fieldMeta.Number);
                    }
                }
            }
        }

        private IEnumerable<OrderByClause.OrderByClauseItem> GetOrderByClauseItems(EntityMeta entityMeta, IEnumerable<OrderByItem> orderByItems)
        {
            foreach (var orderByItem in orderByItems)
            {
                if (orderByItem.Field.Equals(entityMeta.PrimaryKeyName))
                {
                    yield return new OrderByClause.PrimaryKey(orderByItem.Direction);
                }
                else
                {
                    var fieldMeta = entityMeta.FieldMetaCollection.First(x => orderByItem.Field.Equals(x.Name));
                    yield return new OrderByClause.Field(fieldMeta.Number, orderByItem.Direction);
                }
            }
        }
    }
}
