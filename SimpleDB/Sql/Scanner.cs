using System;
using System.Collections.Generic;

namespace SimpleDB.Sql
{
    internal class Scanner
    {
        private readonly SqlQueryReader _rd;
        private List<Token> _tokens;
        private char[] _valueBuffer;
        private int _valueBufferLength;
        private int _row, _col;

        public Scanner(string sqlQuery)
        {
            _rd = new SqlQueryReader(sqlQuery);
        }

        public IEnumerable<Token> GetTokens()
        {
            _tokens = new List<Token>();
            _valueBuffer = new char[256];
            ReadAllTokens();

            return _tokens;
        }

        private void ReadAllTokens()
        {
            _rd.NextChar();
            switch (1)
            {
                case 1:
                    if (_rd.Eof) { AddChar(); AddToken(); }
                    else if (_rd.Char == ' ' || _rd.Char == '\t' || _rd.Char == '\n') { _rd.NextChar(); goto case 1; }
                    else if (Char.IsLetterOrDigit(_rd.Char)) { SetRowCol(); AddChar(); _rd.NextChar(); goto case 2; }
                    else if (_rd.Char == '*') { SetRowCol(); AddChar(); AddToken(); _rd.NextChar(); goto case 1; }
                    else if (_rd.Char == ',') { AddToken(); _rd.NextChar(); goto case 1; }
                    break;
                case 2:
                    if (_rd.Eof) { AddChar(); AddToken(); }
                    else if (_rd.Char == ' ' || _rd.Char == '\t' || _rd.Char == '\n') { AddToken(); _rd.NextChar(); goto case 1; }
                    else if (Char.IsLetterOrDigit(_rd.Char)) { AddChar(); _rd.NextChar(); goto case 2; }
                    else { AddToken(); _rd.NextChar(); goto case 1; }
                    break;
            }
        }

        private void AddChar()
        {
            _valueBuffer[_valueBufferLength++] = _rd.Char;
        }

        private void SetRowCol()
        {
            _row = _rd.Row;
            _col = _rd.Col;
        }

        private void AddToken()
        {
            _tokens.Add(new Token(new string(_valueBuffer, 0, _valueBufferLength), _row, _col));
            _valueBufferLength = 0;
        }
    }
}
