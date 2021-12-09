using System.Linq;
using NUnit.Framework;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql
{
    class ScannerTest
    {
        [SetUp]
        public void Setup()
        {
        }

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

            Assert.AreEqual(6, tokens.Count);

            Assert.AreEqual("SELECT", tokens[0].Value);
            Assert.AreEqual(0, tokens[0].Row);
            Assert.AreEqual(0, tokens[0].Col);

            Assert.AreEqual("Field1", tokens[1].Value);
            Assert.AreEqual(0, tokens[1].Row);
            Assert.AreEqual(7, tokens[1].Col);

            Assert.AreEqual("Field2", tokens[2].Value);
            Assert.AreEqual(0, tokens[2].Row);
            Assert.AreEqual(15, tokens[2].Col);

            Assert.AreEqual("Field3", tokens[3].Value);
            Assert.AreEqual(0, tokens[3].Row);
            Assert.AreEqual(23, tokens[3].Col);

            Assert.AreEqual("FROM", tokens[4].Value);
            Assert.AreEqual(0, tokens[4].Row);
            Assert.AreEqual(30, tokens[4].Col);

            Assert.AreEqual("Table", tokens[5].Value);
            Assert.AreEqual(0, tokens[5].Row);
            Assert.AreEqual(35, tokens[5].Col);
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
    }
}
