using System;
using System.Collections.Generic;
using System.Text;
using NUnit.Framework;
using SimpleDB.Sql;

namespace SimpleDB.Test.Sql
{
    class SqlQueryReaderTest
    {
        [Test]
        public void CurrentChar()
        {
            var reader = new SqlQueryReader("SELECT");
            Assert.AreEqual('S', reader.CurrentChar);
            reader.NextChar();
            Assert.AreEqual('E', reader.CurrentChar);
            reader.NextChar();
            Assert.AreEqual('L', reader.CurrentChar);
            reader.NextChar();
            Assert.AreEqual('E', reader.CurrentChar);
            reader.NextChar();
            Assert.AreEqual('C', reader.CurrentChar);
            reader.NextChar();
            Assert.AreEqual('T', reader.CurrentChar);
            Assert.IsTrue(reader.Eof);
        }

        [Test]
        public void ReadValue_1()
        {
            var reader = new SqlQueryReader("SELECT");
            reader.StartReadValue();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.EndReadValue();
            var value = reader.Value;
            Assert.AreEqual("SELECT", value.Value);
            Assert.AreEqual(0, value.Row);
            Assert.AreEqual(0, value.Col);
        }

        [Test]
        public void ReadValue_2()
        {
            var reader = new SqlQueryReader("SELECT *");
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();

            reader.NextChar();
            
            reader.NextChar();
            reader.StartReadValue();
            reader.EndReadValue();
            var value = reader.Value;
            Assert.AreEqual("*", value.Value);
            Assert.AreEqual(0, value.Row);
            Assert.AreEqual(7, value.Col);
        }

        [Test]
        public void ReadValue_3()
        {
            var reader = new SqlQueryReader("SELECT *\r\nFROM");
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();

            reader.NextChar();
            reader.NextChar();
            reader.NextChar();

            reader.StartReadValue();
            reader.NextChar();
            reader.NextChar();
            reader.NextChar();
            reader.EndReadValue();
            var value = reader.Value;
            Assert.AreEqual("FROM", value.Value);
            Assert.AreEqual(1, value.Row);
            Assert.AreEqual(0, value.Col);
        }
    }
}
