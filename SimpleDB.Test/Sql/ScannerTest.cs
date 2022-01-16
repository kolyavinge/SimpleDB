using System.Linq;
using NUnit.Framework;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql
{
    class ScannerTest
    {
        [SetUp]
        public void Setup() { }

        [Test]
        public void GetTokens_SelectAllFrom()
        {
            var scanner = new Scanner("SELECT * FROM Table");
            var tokens = scanner.GetTokens().ToList();

            Assert.AreEqual(4, tokens.Count);

            Assert.AreEqual("SELECT", tokens[0].Value);
            Assert.AreEqual(0, tokens[0].Row);
            Assert.AreEqual(0, tokens[0].Col);

            Assert.AreEqual("*", tokens[1].Value);
            Assert.AreEqual(0, tokens[1].Row);
            Assert.AreEqual(7, tokens[1].Col);

            Assert.AreEqual("FROM", tokens[2].Value);
            Assert.AreEqual(0, tokens[2].Row);
            Assert.AreEqual(9, tokens[2].Col);

            Assert.AreEqual("Table", tokens[3].Value);
            Assert.AreEqual(0, tokens[3].Row);
            Assert.AreEqual(14, tokens[3].Col);
        }

        [Test]
        public void GetTokens_SelectFieldsFrom()
        {
            var scanner = new Scanner("SELECT Field1, Field2, Field3 FROM Table");
            var tokens = scanner.GetTokens().ToList();

            Assert.AreEqual(8, tokens.Count);

            Assert.AreEqual("SELECT", tokens[0].Value);
            Assert.AreEqual(0, tokens[0].Row);
            Assert.AreEqual(0, tokens[0].Col);

            Assert.AreEqual("Field1", tokens[1].Value);
            Assert.AreEqual(0, tokens[1].Row);
            Assert.AreEqual(7, tokens[1].Col);

            Assert.AreEqual(",", tokens[2].Value);
            Assert.AreEqual(0, tokens[2].Row);
            Assert.AreEqual(13, tokens[2].Col);

            Assert.AreEqual("Field2", tokens[3].Value);
            Assert.AreEqual(0, tokens[3].Row);
            Assert.AreEqual(15, tokens[3].Col);

            Assert.AreEqual(",", tokens[4].Value);
            Assert.AreEqual(0, tokens[4].Row);
            Assert.AreEqual(21, tokens[4].Col);

            Assert.AreEqual("Field3", tokens[5].Value);
            Assert.AreEqual(0, tokens[5].Row);
            Assert.AreEqual(23, tokens[5].Col);

            Assert.AreEqual("FROM", tokens[6].Value);
            Assert.AreEqual(0, tokens[6].Row);
            Assert.AreEqual(30, tokens[6].Col);

            Assert.AreEqual("Table", tokens[7].Value);
            Assert.AreEqual(0, tokens[7].Row);
            Assert.AreEqual(35, tokens[7].Col);
        }

        [Test]
        public void GetTokens_SelectAllFromLines()
        {
            var scanner = new Scanner("SELECT *\r\nFROM Table");
            var tokens = scanner.GetTokens().ToList();

            Assert.AreEqual(4, tokens.Count);

            Assert.AreEqual("SELECT", tokens[0].Value);
            Assert.AreEqual(0, tokens[0].Row);
            Assert.AreEqual(0, tokens[0].Col);

            Assert.AreEqual("*", tokens[1].Value);
            Assert.AreEqual(0, tokens[1].Row);
            Assert.AreEqual(7, tokens[1].Col);

            Assert.AreEqual("FROM", tokens[2].Value);
            Assert.AreEqual(1, tokens[2].Row);
            Assert.AreEqual(0, tokens[2].Col);

            Assert.AreEqual("Table", tokens[3].Value);
            Assert.AreEqual(1, tokens[3].Row);
            Assert.AreEqual(5, tokens[3].Col);
        }

        [Test]
        public void GetTokens_Return()
        {
            var scanner = new Scanner("SELECT * FROM Table\nWHERE x = 1");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(8, tokens.Count);
        }

        [Test]
        public void GetTokens_NoSpaces()
        {
            var scanner = new Scanner("SELECT * FROM Table\nWHERE x=1");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(8, tokens.Count);
        }

        [Test]
        public void GetTokens_Kinds_1()
        {
            var scanner = new Scanner("SELECT * FROM Table");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.SelectKeyword, tokens[0].Kind);
            Assert.AreEqual(TokenKind.Asterisk, tokens[1].Kind);
            Assert.AreEqual(TokenKind.FromKeyword, tokens[2].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[3].Kind);
        }

        [Test]
        public void GetTokens_Kinds_2()
        {
            var scanner = new Scanner("SELECT Field1, Field2, Field3 FROM Table");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.SelectKeyword, tokens[0].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[1].Kind);
            Assert.AreEqual(TokenKind.Comma, tokens[2].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[3].Kind);
            Assert.AreEqual(TokenKind.Comma, tokens[4].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[5].Kind);
            Assert.AreEqual(TokenKind.FromKeyword, tokens[6].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[7].Kind);
        }

        [Test]
        public void GetTokens_IntegerNumber()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x = 123456");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.SelectKeyword, tokens[0].Kind);
            Assert.AreEqual(TokenKind.Asterisk, tokens[1].Kind);
            Assert.AreEqual(TokenKind.FromKeyword, tokens[2].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[3].Kind);
            Assert.AreEqual(TokenKind.WhereKeyword, tokens[4].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[5].Kind);
            Assert.AreEqual(TokenKind.EqualsOperation, tokens[6].Kind);
            Assert.AreEqual(TokenKind.IntegerNumber, tokens[7].Kind);
        }

        [Test]
        public void GetTokens_FloatNumber()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x = 123.456");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.SelectKeyword, tokens[0].Kind);
            Assert.AreEqual(TokenKind.Asterisk, tokens[1].Kind);
            Assert.AreEqual(TokenKind.FromKeyword, tokens[2].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[3].Kind);
            Assert.AreEqual(TokenKind.WhereKeyword, tokens[4].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[5].Kind);
            Assert.AreEqual(TokenKind.EqualsOperation, tokens[6].Kind);
            Assert.AreEqual(TokenKind.FloatNumber, tokens[7].Kind);
        }

        [Test]
        public void GetTokens_String()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x = 'string 123'");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(8, tokens.Count);
            Assert.AreEqual(TokenKind.SelectKeyword, tokens[0].Kind);
            Assert.AreEqual(TokenKind.Asterisk, tokens[1].Kind);
            Assert.AreEqual(TokenKind.FromKeyword, tokens[2].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[3].Kind);
            Assert.AreEqual(TokenKind.WhereKeyword, tokens[4].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[5].Kind);
            Assert.AreEqual(TokenKind.EqualsOperation, tokens[6].Kind);
            Assert.AreEqual(TokenKind.String, tokens[7].Kind);
        }

        [Test]
        public void GetTokens_IN()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x IN (1, 2, 3)");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.SelectKeyword, tokens[0].Kind);
            Assert.AreEqual(TokenKind.Asterisk, tokens[1].Kind);
            Assert.AreEqual(TokenKind.FromKeyword, tokens[2].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[3].Kind);
            Assert.AreEqual(TokenKind.WhereKeyword, tokens[4].Kind);
            Assert.AreEqual(TokenKind.Identificator, tokens[5].Kind);
            Assert.AreEqual(TokenKind.InOperation, tokens[6].Kind);
            Assert.AreEqual(TokenKind.OpenBracket, tokens[7].Kind);
            Assert.AreEqual(TokenKind.IntegerNumber, tokens[8].Kind);
            Assert.AreEqual(TokenKind.Comma, tokens[9].Kind);
            Assert.AreEqual(TokenKind.IntegerNumber, tokens[10].Kind);
            Assert.AreEqual(TokenKind.Comma, tokens[11].Kind);
            Assert.AreEqual(TokenKind.IntegerNumber, tokens[12].Kind);
            Assert.AreEqual(TokenKind.CloseBracket, tokens[13].Kind);
        }

        [Test]
        public void GetTokens_Great()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x > 1");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.GreatOperation, tokens[6].Kind);
        }

        [Test]
        public void GetTokens_Less()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x < 1");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.LessOperation, tokens[6].Kind);
        }

        [Test]
        public void GetTokens_GreatOrEquals()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x >= 1");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.GreatOrEqualsOperation, tokens[6].Kind);
        }

        [Test]
        public void GetTokens_LessOrEquals()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x <= 1");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.LessOrEqualsOperation, tokens[6].Kind);
        }

        [Test]
        public void GetTokens_NotEquals()
        {
            var scanner = new Scanner("SELECT * FROM Table WHERE x != 1");
            var tokens = scanner.GetTokens().ToList();
            Assert.AreEqual(TokenKind.NotEqualsOperation, tokens[6].Kind);
        }
    }
}
