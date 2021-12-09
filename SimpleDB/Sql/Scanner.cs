using System;
using System.Collections.Generic;

namespace SimpleDB.Sql
{
    internal class Scanner
    {
        private readonly string _sqlQuery;
        private char[] _value;
        private int _valueIndex;
        private int _charIndex;
        private int _charCol;
        private int _charRow;
        private char _ch;
        private bool _eof;

        public Scanner(string sqlQuery)
        {
            _value = new char[256];
            _sqlQuery = sqlQuery;
            _ch = _sqlQuery[0];
        }

        public IEnumerable<Token> GetTokens()
        {
            var token = GetNextToken();
            while (token != null)
            {
                yield return token;
                token = GetNextToken();
            }
        }

        private Token GetNextToken()
        {
            if (_eof) return null;
            _valueIndex = 0;
            var startCol = _charCol;
            switch (1)
            {
                case 1:
                    if (_eof) break;
                    else if (_ch == ' ' || _ch == '\t') { NextChar(); break; }
                    else if (_ch == '\n') { _charRow++; _charCol = 0; startCol = _charCol; NextChar(); goto case 1; }
                    else if (_ch == '\r') { _charCol = 0; startCol = _charCol; NextChar(); goto case 1; }
                    else if (Char.IsLetterOrDigit(_ch)) { AddChar(); NextChar(); goto case 1; }
                    else if (_ch == '*') { AddChar(); NextChar(); break; }
                    else if (_ch == ',') { NextChar(); break; }
                    break;
            }

            return new Token(new string(_value, 0, _valueIndex), _charRow, startCol);
        }

        private void NextChar()
        {
            _charIndex++;
            _eof = _charIndex == _sqlQuery.Length;
            if (!_eof)
            {
                _ch = _sqlQuery[_charIndex];
                _charCol++;
            }
        }

        private void AddChar()
        {
            _value[_valueIndex] = _ch;
            _valueIndex++;
        }
    }
}
