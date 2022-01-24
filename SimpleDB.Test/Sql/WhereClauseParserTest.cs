using System.Collections.Generic;
using NUnit.Framework;
using SimpleDB.Core;
using SimpleDB.Queries;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql
{
    class WhereClauseParserTest
    {
        private EntityMeta _entityMeta;
        private WhereClauseParser _parser;

        [SetUp]
        public void Setup()
        {
            _entityMeta = new EntityMeta
            {
                EntityName = "User",
                PrimaryKeyName = "Id",
                FieldMetaCollection = new FieldMeta[]
                {
                    new FieldMeta(0, "Login", typeof(string)),
                    new FieldMeta(1, "Name", typeof(string)),
                    new FieldMeta(2, "Age", typeof(int)),
                    new FieldMeta(3, "Float", typeof(float)),
                }
            };
            _parser = new WhereClauseParser();
        }

        [Test]
        public void NoWhere_Null()
        {
            var tokens = new List<Token>
            {
                new Token("SELECT", TokenKind.SelectKeyword, 0, 0),
                new Token("*", TokenKind.Identificator, 0, 0),
                new Token("FROM", TokenKind.WhereKeyword, 0, 0),
                new Token("User", TokenKind.Identificator, 0, 0),
            };
            try
            {
                GetClause(tokens);
                Assert.Fail();
            }
            catch (InvalidQueryException)
            {
            }
        }

        [Test]
        public void EqualsOperation_FieldAndPrimaryKey()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Id", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.PrimaryKey), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), root.Right.GetType());
            Assert.AreEqual(123, root.Right.Value);
        }

        [Test]
        public void EqualsOperation_FieldAndIntegerNumber()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.Field), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), root.Right.GetType());
            Assert.AreEqual(2, root.Left.Number);
            Assert.AreEqual(123, root.Right.Value);
        }

        [Test]
        public void EqualsOperation_FieldAndString()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.Field), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Constant), root.Right.GetType());
            Assert.AreEqual(1, root.Left.Number);
            Assert.AreEqual("user name", root.Right.Value);
        }

        [Test]
        public void EqualsOperation_FieldAndSet()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("IN", TokenKind.InOperation, 0, 0),
                new Token("(", TokenKind.OpenBracket, 0, 0),
                new Token("1", TokenKind.String, 0, 0),
                new Token(",", TokenKind.Comma, 0, 0),
                new Token("2", TokenKind.String, 0, 0),
                new Token(",", TokenKind.Comma, 0, 0),
                new Token("3", TokenKind.String, 0, 0),
                new Token(")", TokenKind.CloseBracket, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.InOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.Field), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.Set), root.Right.GetType());
            Assert.AreEqual(1, root.Left.Number);
            Assert.AreEqual(3, root.Right.Value.Count);
        }

        [Test]
        public void OrOperation_TwoFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Right.Value);
            Assert.AreEqual(2, root.Right.Left.Number);
            Assert.AreEqual(123, root.Right.Right.Value);
        }

        [Test]
        public void AndOperation_TwoFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Right.Value);
            Assert.AreEqual(2, root.Right.Left.Number);
            Assert.AreEqual(123, root.Right.Right.Value);
        }

        [Test]
        public void OrOperation_ThreeFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("login", TokenKind.String, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Right.Value);
            Assert.AreEqual(2, root.Right.Left.Left.Number);
            Assert.AreEqual(123, root.Right.Left.Right.Value);
            Assert.AreEqual(0, root.Right.Right.Left.Number);
            Assert.AreEqual("login", root.Right.Right.Right.Value);
        }

        [Test]
        public void AndOperation_ThreeFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("login", TokenKind.String, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Right.Value);
            Assert.AreEqual(2, root.Right.Left.Left.Number);
            Assert.AreEqual(123, root.Right.Left.Right.Value);
            Assert.AreEqual(0, root.Right.Right.Left.Number);
            Assert.AreEqual("login", root.Right.Right.Right.Value);
        }

        [Test]
        public void OrAndOperation_1_ThreeFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("login", TokenKind.String, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Right.Value);
            Assert.AreEqual(2, root.Right.Left.Left.Number);
            Assert.AreEqual(123, root.Right.Left.Right.Value);
            Assert.AreEqual(0, root.Right.Right.Left.Number);
            Assert.AreEqual("login", root.Right.Right.Right.Value);
        }

        [Test]
        public void OrAndOperation_2_ThreeFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("login", TokenKind.String, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Left.Right.Value);
            Assert.AreEqual(2, root.Left.Right.Left.Number);
            Assert.AreEqual(123, root.Left.Right.Right.Value);
            Assert.AreEqual(0, root.Right.Left.Number);
            Assert.AreEqual("login", root.Right.Right.Value);
        }

        [Test]
        public void Brackets_1_ThreeFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("(", TokenKind.OpenBracket, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("login", TokenKind.String, 0, 0),
                new Token(")", TokenKind.CloseBracket, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Right.Value);
            Assert.AreEqual(2, root.Right.Left.Left.Number);
            Assert.AreEqual(123, root.Right.Left.Right.Value);
            Assert.AreEqual(0, root.Right.Right.Left.Number);
            Assert.AreEqual("login", root.Right.Right.Right.Value);
        }

        [Test]
        public void Brackets_2_ThreeFields()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("(", TokenKind.OpenBracket, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
                new Token(")", TokenKind.CloseBracket, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Login", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("login", TokenKind.String, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Right.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.Left.Right.GetType());
            Assert.AreEqual(1, root.Left.Left.Left.Number);
            Assert.AreEqual("user name", root.Left.Left.Right.Value);
            Assert.AreEqual(2, root.Left.Right.Left.Number);
            Assert.AreEqual(123, root.Left.Right.Right.Value);
            Assert.AreEqual(0, root.Right.Left.Number);
            Assert.AreEqual("login", root.Right.Right.Value);
        }

        [Test]
        public void FloatField()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("Float", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123.456", TokenKind.FloatNumber, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.EqualsOperation), root.GetType());
            Assert.AreEqual(123.456, root.Right.Value);
        }

        [Test]
        public void Brackets_3()
        {
            var tokens = new List<Token>
            {
                new Token("WHERE", TokenKind.WhereKeyword, 0, 0),
                new Token("(", TokenKind.OpenBracket, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("user name", TokenKind.String, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("123", TokenKind.IntegerNumber, 0, 0),
                new Token(")", TokenKind.CloseBracket, 0, 0),
                new Token("OR", TokenKind.OrOperation, 0, 0),
                new Token("(", TokenKind.OpenBracket, 0, 0),
                new Token("Name", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("name user", TokenKind.String, 0, 0),
                new Token("AND", TokenKind.AndOperation, 0, 0),
                new Token("Age", TokenKind.Identificator, 0, 0),
                new Token("=", TokenKind.EqualsOperation, 0, 0),
                new Token("321", TokenKind.IntegerNumber, 0, 0),
                new Token(")", TokenKind.CloseBracket, 0, 0),
            };
            dynamic root = GetClause(tokens);
            Assert.AreEqual(typeof(WhereClause.OrOperation), root.GetType());
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.Left.GetType());
            Assert.AreEqual(typeof(WhereClause.AndOperation), root.Right.GetType());
        }

        private dynamic GetClause(IEnumerable<Token> tokens)
        {
            return _parser.GetClause(_entityMeta, new TokenIterator(tokens)).Root;
        }
    }
}
