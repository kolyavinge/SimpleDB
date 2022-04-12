using System;
using System.Collections.Generic;

namespace SimpleDB.Sql
{
    class TokenIterator
    {
        private readonly IEnumerator<Token> _iterator;

        public TokenIterator(IEnumerable<Token> tokens)
        {
            Current = new Token("", TokenKind.EqualsOperation, 0, 0); // dummy
            _iterator = tokens.GetEnumerator();
            Eof = false;
            NextToken();
        }

        public Token Current { get; private set; }

        public bool Eof { get; private set; }

        public void NextToken()
        {
            if (_iterator.MoveNext())
            {
                Current = _iterator.Current ?? throw new NullReferenceException();
            }
            else
            {
                Eof = true;
            }
        }
    }
}
