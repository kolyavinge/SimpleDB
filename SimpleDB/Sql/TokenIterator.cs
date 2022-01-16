using System;
using System.Collections.Generic;
using System.Text;

namespace SimpleDB.Sql
{
    class TokenIterator
    {
        private IEnumerator<Token> _iterator;

        public TokenIterator(IEnumerable<Token> tokens)
        {
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
                Current = _iterator.Current;
            }
            else
            {
                Eof = true;
            }
        }
    }
}
