using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using SimpleDB.Core;
using SimpleDB.Queries;

namespace SimpleDB.Sql;

internal class WhereClauseParser
{
    private readonly EntityMeta _entityMeta;
    private readonly TokenIterator _tokenIterator;

    public WhereClauseParser(EntityMeta entityMeta, TokenIterator tokenIterator)
    {
        if (tokenIterator.Current.Kind != TokenKind.WhereKeyword) throw new InvalidQueryException();
        _entityMeta = entityMeta;
        _tokenIterator = tokenIterator;
    }

    public WhereClause GetClause()
    {
        _tokenIterator.NextToken();
        var root = Exp1();

        return new WhereClause(root);
    }

    private WhereClause.WhereClauseItem Exp1()
    {
        var left = Exp2();
        if (!_tokenIterator.Eof)
        {
            if (_tokenIterator.Current.Kind == TokenKind.OrOperation)
            {

                _tokenIterator.NextToken();
                var right = Exp1();
                return new WhereClause.OrOperation(left, right);
            }
        }

        return left;
    }

    private WhereClause.WhereClauseItem Exp2()
    {
        WhereClause.WhereClauseItem left;
        if (_tokenIterator.Current.Kind == TokenKind.OpenBracket)
        {
            _tokenIterator.NextToken();
            left = Exp1();
            if (_tokenIterator.Current.Kind != TokenKind.CloseBracket) throw new InvalidQueryException();
        }
        else
        {
            left = Exp3();
        }
        _tokenIterator.NextToken();
        if (!_tokenIterator.Eof)
        {
            if (_tokenIterator.Current.Kind == TokenKind.AndOperation)
            {
                _tokenIterator.NextToken();
                var right = Exp2();
                return new WhereClause.AndOperation(left, right);
            }
        }

        return left;
    }

    private WhereClause.WhereClauseItem Exp3()
    {
        var left = Atom();
        _tokenIterator.NextToken();
        var op = _tokenIterator.Current;
        _tokenIterator.NextToken();
        var right = Atom();
        if (op.Kind == TokenKind.EqualsOperation) return new WhereClause.EqualsOperation(left, right);
        if (op.Kind == TokenKind.NotEqualsOperation) return new WhereClause.NotOperation(new WhereClause.EqualsOperation(left, right));
        if (op.Kind == TokenKind.GreatOperation) return new WhereClause.GreatOperation(left, right);
        if (op.Kind == TokenKind.LessOperation) return new WhereClause.LessOperation(left, right);
        if (op.Kind == TokenKind.GreatOrEqualsOperation) return new WhereClause.GreatOrEqualsOperation(left, right);
        if (op.Kind == TokenKind.LessOrEqualsOperation) return new WhereClause.LessOrEqualsOperation(left, right);
        if (op.Kind == TokenKind.InOperation)
        {
            if (!(right is WhereClause.Set)) throw new InvalidQueryException();
            return new WhereClause.InOperation(left, (WhereClause.Set)right);
        }
        if (op.Kind == TokenKind.LikeOperation)
        {
            if (!(right is WhereClause.Constant)) throw new InvalidQueryException();
            return new WhereClause.LikeOperation(left, (WhereClause.Constant)right);
        }

        throw new InvalidQueryException();
    }

    private WhereClause.WhereClauseItem Atom()
    {
        if (_tokenIterator.Current.Kind == TokenKind.IntegerNumber)
        {
            return new WhereClause.Constant(Int32.Parse(_tokenIterator.Current.Value));
        }

        if (_tokenIterator.Current.Kind == TokenKind.FloatNumber)
        {
            return new WhereClause.Constant(Double.Parse(_tokenIterator.Current.Value, new NumberFormatInfo { NumberDecimalSeparator = "." }));
        }

        if (_tokenIterator.Current.Kind == TokenKind.String)
        {
            return new WhereClause.Constant(_tokenIterator.Current.Value);
        }

        if (_tokenIterator.Current.Kind == TokenKind.OpenBracket)
        {
            return Set();
        }

        if (_tokenIterator.Current.Value.Equals(_entityMeta.PrimaryKeyFieldMeta.Name))
        {
            return new WhereClause.PrimaryKey();
        }

        var field = _entityMeta.FieldMetaCollection.FirstOrDefault(f => _tokenIterator.Current.Value.Equals(f.Name));
        if (field is null) throw new InvalidQueryException();
        return new WhereClause.Field(field.Number);
    }

    private WhereClause.Set Set()
    {
        _tokenIterator.NextToken();
        var items = new List<object>();
        switch (1)
        {
            case 1:
                if (_tokenIterator.Current.Kind == TokenKind.IntegerNumber) items.Add(Int32.Parse(_tokenIterator.Current.Value));
                else if (_tokenIterator.Current.Kind == TokenKind.FloatNumber) items.Add(Double.Parse(_tokenIterator.Current.Value));
                else if (_tokenIterator.Current.Kind == TokenKind.String) items.Add(_tokenIterator.Current.Value);
                else throw new InvalidQueryException();
                _tokenIterator.NextToken();
                goto case 2;
            case 2:
                if (_tokenIterator.Current.Kind == TokenKind.CloseBracket) { _tokenIterator.NextToken(); break; }
                else if (_tokenIterator.Current.Kind == TokenKind.Comma) { _tokenIterator.NextToken(); goto case 1; }
                else throw new InvalidQueryException();
        }

        return new WhereClause.Set(items);
    }
}
