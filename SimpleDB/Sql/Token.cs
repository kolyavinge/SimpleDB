using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Sql
{
    internal class Token
    {
        public readonly string Value;
        public readonly int Row;
        public readonly int Col;

        public Token(string value, int row, int col)
        {
            Value = value;
            Row = row;
            Col = col;
        }

        public Token(string value) : this(value, 0, 0) { }

        public bool IsValueEquals(string x)
        {
            return String.Equals(Value, x, StringComparison.OrdinalIgnoreCase);
        }
    }
}
