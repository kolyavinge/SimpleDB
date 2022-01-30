using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql
{
    class UpdateQueryParserTest
    {
        private QueryContext _context;
        private UpdateQueryParser _parser;

        [SetUp]
        public void Setup()
        {
            _context = new QueryContext
            {
                EntityMetaDictionary = new Dictionary<string, EntityMeta>
                {
                    {
                        "User",
                        new EntityMeta
                        {
                            EntityName = "User",
                            PrimaryKeyFieldMeta = new PrimaryKeyFieldMeta("Id", typeof(int)),
                            FieldMetaCollection = new[]
                            {
                                new FieldMeta(1, "Login", typeof(string)),
                                new FieldMeta(2, "Name", typeof(string)),
                                new FieldMeta(3, "Byte", typeof(byte)),
                                new FieldMeta(4, "Float", typeof(float))
                            }
                        }
                    }
                }
            };
            _parser = new UpdateQueryParser();
        }

        [Test]
        public void UpdateOneField()
        {
            var tokens = new List<Token>
            {
                new Token("UPDATE", TokenKind.UpdateKeyword, 0, 0),
                new Token("User", TokenKind.Identificator, 0, 0),
                new Token("SET", TokenKind.SetKeyword, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.String, 0, 0)
            };
            var query = _parser.GetQuery(_context, tokens) as UpdateQuery;
            Assert.AreEqual("User", query.EntityName);
            var updatedItems = query.UpdateClause.UpdateItems.ToList();
            Assert.AreEqual(1, updatedItems.Count);
            Assert.IsTrue(updatedItems[0] is UpdateClause.Field);
            var field = (UpdateClause.Field)updatedItems[0];
            Assert.AreEqual(1, field.Number);
            Assert.AreEqual("123", field.Value);
        }

        [Test]
        public void UpdateTwoFields()
        {
            var tokens = new List<Token>
            {
                new Token("UPDATE", TokenKind.UpdateKeyword, 0, 0),
                new Token("User", TokenKind.Identificator, 0, 0),
                new Token("SET", TokenKind.SetKeyword, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.String, 0, 0),
                new Token(",", TokenKind.Comma, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("456", TokenKind.String, 0, 0),
            };
            var query = _parser.GetQuery(_context, tokens) as UpdateQuery;
            Assert.AreEqual("User", query.EntityName);
            var updatedItems = query.UpdateClause.UpdateItems.ToList();
            Assert.AreEqual(2, updatedItems.Count);
            Assert.IsTrue(updatedItems[0] is UpdateClause.Field);
            var field = (UpdateClause.Field)updatedItems[0];
            Assert.AreEqual(1, field.Number);
            Assert.AreEqual("123", field.Value);
            field = (UpdateClause.Field)updatedItems[1];
            Assert.AreEqual(2, field.Number);
            Assert.AreEqual("456", field.Value);
        }

        [Test]
        public void UpdateOneFieldWhere()
        {
            var tokens = new List<Token>
            {
                new Token("UPDATE", TokenKind.UpdateKeyword, 0, 0),
                new Token("User", TokenKind.Identificator, 0, 0),
                new Token("SET", TokenKind.SetKeyword, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.String, 0, 0),
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("789", TokenKind.String, 0, 0)
            };
            var query = _parser.GetQuery(_context, tokens) as UpdateQuery;
            Assert.NotNull(query.WhereClause);
            dynamic root = query.WhereClause.Root;
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.Field), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), root.Right.GetType());
        }

        [Test]
        public void UpdateTwoFieldsWhere()
        {
            var tokens = new List<Token>
            {
                new Token("UPDATE", TokenKind.UpdateKeyword, 0, 0),
                new Token("User", TokenKind.Identificator, 0, 0),
                new Token("SET", TokenKind.SetKeyword, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.String, 0, 0),
                new Token(",", TokenKind.Comma, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("456", TokenKind.String, 0, 0),
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("789", TokenKind.String, 0, 0)
            };
            var query = _parser.GetQuery(_context, tokens) as UpdateQuery;
            Assert.NotNull(query.WhereClause);
            dynamic root = query.WhereClause.Root;
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.Field), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), root.Right.GetType());
        }

        [Test]
        public void Update_ConvertFieldByte()
        {
            var tokens = new List<Token>
            {
                new Token("UPDATE", TokenKind.UpdateKeyword, 0, 0),
                new Token("User", TokenKind.Identificator, 0, 0),
                new Token("SET", TokenKind.SetKeyword, 0, 0),
                new Token("Byte", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0)
            };
            var query = _parser.GetQuery(_context, tokens) as UpdateQuery;
            var item = query.UpdateClause.UpdateItems.First() as UpdateClause.Field;
            Assert.AreEqual(typeof(byte), item.Value.GetType());
        }

        [Test]
        public void Update_ConvertFieldFloat()
        {
            var tokens = new List<Token>
            {
                new Token("UPDATE", TokenKind.UpdateKeyword, 0, 0),
                new Token("User", TokenKind.Identificator, 0, 0),
                new Token("SET", TokenKind.SetKeyword, 0, 0),
                new Token("Float", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123.456", TokenKind.FloatNumber, 0, 0)
            };
            var query = _parser.GetQuery(_context, tokens) as UpdateQuery;
            var item = query.UpdateClause.UpdateItems.First() as UpdateClause.Field;
            Assert.AreEqual(typeof(float), item.Value.GetType());
        }

        [Test]
        public void UpdateWrongTable()
        {
            var tokens = new List<Token>
            {
                new Token("UPDATE", TokenKind.UpdateKeyword, 0, 0),
                new Token("WRONG_TABLE", TokenKind.Identificator, 0, 0),
                new Token("SET", TokenKind.SetKeyword, 0, 0),
                new Token("Float", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123.456", TokenKind.FloatNumber, 0, 0)
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
}
