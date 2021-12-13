using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql
{
    class SelectQueryParserTest
    {
        private QueryContext _context;
        private SelectQueryParser _parser;

        [SetUp]
        public void Setup()
        {
            _context = new QueryContext
            {
                EntityMetaCollection = new List<EntityMeta>
                {
                    new EntityMeta
                    {
                        EntityName = "User",
                        PrimaryKeyName = "Id",
                        FieldMetaCollection = new FieldMeta[]
                        {
                            new FieldMeta(0, "Login", typeof(string)),
                            new FieldMeta(1, "Name", typeof(string))
                        }
                    }
                }
            };
            _parser = new SelectQueryParser();
        }

        [Test]
        public void SelectAll()
        {
            var tokens = new List<Token>
            {
                new Token("SELECT"),
                new Token("*"),
                new Token("FROM"),
                new Token("User")
            };
            var query = _parser.GetQuery(_context, tokens) as SelectQuery;
            Assert.AreEqual("User", query.EntityName);
            var selectItems = query.SelectClause.SelectItems.ToList();
            Assert.AreEqual(3, selectItems.Count);
            Assert.IsTrue(selectItems[0] is SelectClause.PrimaryKey);
            var fields = selectItems.Skip(1).Cast<SelectClause.Field>().ToList();
            Assert.AreEqual(0, fields[0].Number);
            Assert.AreEqual(1, fields[1].Number);
        }

        [Test]
        public void SelectId()
        {
            var tokens = new List<Token>
            {
                new Token("SELECT"),
                new Token("Id"),
                new Token("FROM"),
                new Token("User")
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
                new Token("SELECT"),
                new Token("Id"),
                new Token("Login"),
                new Token("Name"),
                new Token("FROM"),
                new Token("User")
            };
            var query = _parser.GetQuery(_context, tokens) as SelectQuery;
            Assert.AreEqual("User", query.EntityName);
            var selectItems = query.SelectClause.SelectItems.ToList();
            Assert.AreEqual(3, selectItems.Count);
            Assert.IsTrue(selectItems[0] is SelectClause.PrimaryKey);
            var fields = selectItems.Skip(1).Cast<SelectClause.Field>().ToList();
            Assert.AreEqual(0, fields[0].Number);
            Assert.AreEqual(1, fields[1].Number);
        }
    }
}
