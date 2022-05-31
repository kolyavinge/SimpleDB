using System.Collections.Generic;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql;

class DeleteQueryParserTest
{
    private QueryContext _context;
    private DeleteQueryParser _parser;

    [SetUp]
    public void Setup()
    {
        _context = new QueryContext(
            new Dictionary<string, EntityMeta>
            {
                {
                    "User",
                    new EntityMeta("User",
                        new PrimaryKeyFieldMeta("Id", typeof(int)),
                        new[]
                        {
                            new FieldMeta(1, "Login", typeof(string)),
                            new FieldMeta(2, "Name", typeof(string))
                        })
                }
            });
        _parser = new DeleteQueryParser();
    }

    [Test]
    public void DeleteAll()
    {
        var tokens = new List<Token>
        {
            new Token("DELETE", TokenKind.DeleteKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0)
        };
        var query = _parser.GetQuery(_context, tokens) as DeleteQuery;
        Assert.AreEqual("User", query.EntityName);
        Assert.IsNull(query.WhereClause);
    }

    [Test]
    public void DeleteWhere()
    {
        var tokens = new List<Token>
        {
            new Token("DELETE", TokenKind.DeleteKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
            new Token("Login", TokenKind.Identificator, 0, 0),
            new Token("=", TokenKind.EqualsOperation, 0, 0),
            new Token("789", TokenKind.String, 0, 0)
        };
        var query = _parser.GetQuery(_context, tokens) as DeleteQuery;
        Assert.AreEqual("User", query.EntityName);
        Assert.NotNull(query.WhereClause);
        dynamic root = query.WhereClause.Root;
        Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
        Assert.AreEqual(typeof(WhereClause.Field), root.Left.GetType());
        Assert.AreEqual(typeof(WhereClause.Constant), root.Right.GetType());
    }

    [Test]
    public void DeleteWrongTable()
    {
        var tokens = new List<Token>
        {
            new Token("DELETE", TokenKind.DeleteKeyword, 0, 0),
            new Token("WRONG_TABLE", TokenKind.Identificator, 0, 0)
        };
        try
        {
            _parser.GetQuery(_context, tokens);
            Assert.Fail();
        }
        catch (InvalidQueryException)
        {
        }
    }
}
