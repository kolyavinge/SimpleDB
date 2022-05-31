using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql;

class SelectQueryParserTest
{
    private QueryContext _context;
    private SelectQueryParser _parser;

    [SetUp]
    public void Setup()
    {
        _context = new QueryContext(
            new Dictionary<string, EntityMeta>
            {
                {
                    "User",
                    new EntityMeta(
                        "User",
                        new PrimaryKeyFieldMeta("Id", typeof(int)),
                        new[]
                        {
                            new FieldMeta(1, "Login", typeof(string)),
                            new FieldMeta(2, "Name", typeof(string))
                        })
                }
            });
        _parser = new SelectQueryParser();
    }

    [Test]
    public void SelectAll()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Asterisk, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0)
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual("User", query.EntityName);
        var selectItems = query.SelectClause.SelectItems.ToList();
        Assert.AreEqual(3, selectItems.Count);
        Assert.IsTrue(selectItems[0] is SelectClause.PrimaryKey);
        var fields = selectItems.Skip(1).Cast<SelectClause.Field>().ToList();
        Assert.AreEqual(1, fields[0].Number);
        Assert.AreEqual(2, fields[1].Number);
    }

    [Test]
    public void SelectId()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("Id", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0)
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual("User", query.EntityName);
        var selectItems = query.SelectClause.SelectItems.ToList();
        Assert.AreEqual(1, selectItems.Count);
        Assert.IsTrue(selectItems[0] is SelectClause.PrimaryKey);
    }

    [Test]
    public void SelectFields()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("Id", TokenKind.Identificator, 0, 0),
            new Token(",", TokenKind.Comma, 0, 0),
            new Token("Login", TokenKind.Identificator, 0, 0),
            new Token(",", TokenKind.Comma, 0, 0),
            new Token("Name", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0)
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual("User", query.EntityName);
        var selectItems = query.SelectClause.SelectItems.ToList();
        Assert.AreEqual(3, selectItems.Count);
        Assert.IsTrue(selectItems[0] is SelectClause.PrimaryKey);
        var fields = selectItems.Skip(1).Cast<SelectClause.Field>().ToList();
        Assert.AreEqual(1, fields[0].Number);
        Assert.AreEqual(2, fields[1].Number);
    }

    [Test]
    public void Where()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Asterisk, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
            new Token("Id", TokenKind.Identificator, 0, 0),
            new Token("=", TokenKind.EqualsOperation, 0, 0),
            new Token("1", TokenKind.IntegerNumber, 0, 0)
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        dynamic root = query.WhereClause.Root;
        Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
        Assert.AreEqual(typeof(WhereClause.PrimaryKey), root.Left.GetType());
        Assert.AreEqual(typeof(WhereClause.Constant), root.Right.GetType());
    }

    [Test]
    public void SelectWrongTable()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
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

    [Test]
    public void SelectOrderBy_OneField()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("ORDERBY", TokenKind.OrderByKeyword, 0, 0),
            new Token("Name", TokenKind.Identificator, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(1, query.OrderByClause.OrderedItems.Count());
        var orderedItems = query.OrderByClause.OrderedItems;
        Assert.AreEqual(typeof(OrderByClause.Field), orderedItems.First().GetType());
        Assert.AreEqual(2, (orderedItems.First() as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Asc, orderedItems.First().Direction);
    }

    [Test]
    public void SelectOrderBy_OneFieldDirectionAsc()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("ORDERBY", TokenKind.OrderByKeyword, 0, 0),
            new Token("Name", TokenKind.Identificator, 0, 0),
            new Token("ASC", TokenKind.AscKeyword, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(1, query.OrderByClause.OrderedItems.Count());
        var orderedItems = query.OrderByClause.OrderedItems;
        Assert.AreEqual(typeof(OrderByClause.Field), orderedItems.First().GetType());
        Assert.AreEqual(2, (orderedItems.First() as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Asc, orderedItems.First().Direction);
    }

    [Test]
    public void SelectOrderBy_OneFieldDirectionDesc()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("ORDERBY", TokenKind.OrderByKeyword, 0, 0),
            new Token("Name", TokenKind.Identificator, 0, 0),
            new Token("DESC", TokenKind.DescKeyword, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(1, query.OrderByClause.OrderedItems.Count());
        var orderedItems = query.OrderByClause.OrderedItems;
        Assert.AreEqual(typeof(OrderByClause.Field), orderedItems.First().GetType());
        Assert.AreEqual(2, (orderedItems.First() as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Desc, orderedItems.First().Direction);
    }

    [Test]
    public void SelectOrderBy_TwoFields()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("ORDERBY", TokenKind.OrderByKeyword, 0, 0),
            new Token("Name", TokenKind.Identificator, 0, 0),
            new Token(",", TokenKind.Comma, 0, 0),
            new Token("Login", TokenKind.Identificator, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(2, query.OrderByClause.OrderedItems.Count());
        var firstOrderedItem = query.OrderByClause.OrderedItems.First();
        Assert.AreEqual(typeof(OrderByClause.Field), firstOrderedItem.GetType());
        Assert.AreEqual(2, (firstOrderedItem as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Asc, firstOrderedItem.Direction);
        var secondOrderedItem = query.OrderByClause.OrderedItems.Last();
        Assert.AreEqual(typeof(OrderByClause.Field), secondOrderedItem.GetType());
        Assert.AreEqual(1, (secondOrderedItem as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Asc, secondOrderedItem.Direction);
    }

    [Test]
    public void SelectOrderBy_TwoFieldsOneDirection()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("ORDERBY", TokenKind.OrderByKeyword, 0, 0),
            new Token("Name", TokenKind.Identificator, 0, 0),
            new Token("DESC", TokenKind.DescKeyword, 0, 0),
            new Token(",", TokenKind.Comma, 0, 0),
            new Token("Login", TokenKind.Identificator, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(2, query.OrderByClause.OrderedItems.Count());
        var firstOrderedItem = query.OrderByClause.OrderedItems.First();
        Assert.AreEqual(typeof(OrderByClause.Field), firstOrderedItem.GetType());
        Assert.AreEqual(2, (firstOrderedItem as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Desc, firstOrderedItem.Direction);
        var secondOrderedItem = query.OrderByClause.OrderedItems.Last();
        Assert.AreEqual(typeof(OrderByClause.Field), secondOrderedItem.GetType());
        Assert.AreEqual(1, (secondOrderedItem as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Asc, secondOrderedItem.Direction);
    }

    [Test]
    public void SelectOrderBy_TwoFieldsTwoDirections()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("ORDERBY", TokenKind.OrderByKeyword, 0, 0),
            new Token("Name", TokenKind.Identificator, 0, 0),
            new Token("DESC", TokenKind.DescKeyword, 0, 0),
            new Token(",", TokenKind.Comma, 0, 0),
            new Token("Login", TokenKind.Identificator, 0, 0),
            new Token("DESC", TokenKind.DescKeyword, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(2, query.OrderByClause.OrderedItems.Count());
        var firstOrderedItem = query.OrderByClause.OrderedItems.First();
        Assert.AreEqual(typeof(OrderByClause.Field), firstOrderedItem.GetType());
        Assert.AreEqual(2, (firstOrderedItem as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Desc, firstOrderedItem.Direction);
        var secondOrderedItem = query.OrderByClause.OrderedItems.Last();
        Assert.AreEqual(typeof(OrderByClause.Field), secondOrderedItem.GetType());
        Assert.AreEqual(1, (secondOrderedItem as OrderByClause.Field).Number);
        Assert.AreEqual(SortDirection.Desc, secondOrderedItem.Direction);
    }

    [Test]
    public void SelectSkip()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("SKIP", TokenKind.SkipKeyword, 0, 0),
            new Token("123", TokenKind.IntegerNumber, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(123, query.Skip);
    }

    [Test]
    public void SelectLimit()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("LIMIT", TokenKind.LimitKeyword, 0, 0),
            new Token("123", TokenKind.IntegerNumber, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(123, query.Limit);
    }

    [Test]
    public void SelectSkipLimit()
    {
        var tokens = new List<Token>
        {
            new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
            new Token("*", TokenKind.Identificator, 0, 0),
            new Token("FROM", TokenKind.FromKeyword, 0, 0),
            new Token("User", TokenKind.Identificator, 0, 0),
            new Token("SKIP", TokenKind.SkipKeyword, 0, 0),
            new Token("123", TokenKind.IntegerNumber, 0, 0),
            new Token("LIMIT", TokenKind.LimitKeyword, 0, 0),
            new Token("456", TokenKind.IntegerNumber, 0, 0),
        };
        var query = _parser.GetQuery(_context, tokens) as SelectQuery;
        Assert.AreEqual(123, query.Skip);
        Assert.AreEqual(456, query.Limit);
    }
}
