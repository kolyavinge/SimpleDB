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
                    if (_rd.Eof) { AddToken(); }
                    else if (_rd.Char == ' ' || _rd.Char == '\t' || _rd.Char == '\n') { _rd.NextChar(); goto case 1; }
                    else if (Char.IsDigit(_rd.Char)) { SetRowCol(); AddChar(); _rd.NextChar(); goto case 4; }
                    else if (Char.IsLetterOrDigit(_rd.Char)) { SetRowCol(); AddChar(); _rd.NextChar(); goto case 2; }
                    else if (_rd.Char == '*') { SetRowCol(); AddChar(); AddToken(TokenKind.Asterisk); _rd.NextChar(); goto case 1; }
                    else if (_rd.Char == '=') { AddChar(); AddToken(TokenKind.EqualsOperation); _rd.NextChar(); goto case 1; }
                    else if (_rd.Char == '>') { AddChar(); _rd.NextChar(); goto case 6; }
                    else if (_rd.Char == '<') { AddChar(); _rd.NextChar(); goto case 6; }
                    else if (_rd.Char == '\'') { SetRowCol(); _rd.NextChar(); goto case 3; }
                    else if (_rd.Char == '(') { SetRowCol(); AddChar(); AddToken(TokenKind.OpenBracket); _rd.NextChar(); goto case 1; }
                    else if (_rd.Char == ')') { SetRowCol(); AddChar(); AddToken(TokenKind.CloseBracket); _rd.NextChar(); goto case 1; }
                    else if (_rd.Char == ',') { SetRowCol(); AddChar(); AddToken(TokenKind.Comma); _rd.NextChar(); goto case 1; }
                    else if (_rd.Char == '!') { SetRowCol(); AddChar(); _rd.NextChar(); goto case 7; }
                    break;
                case 2:
                    if (_rd.Eof) { AddToken(); }
                    else if (_rd.Char == ' ' || _rd.Char == '\t' || _rd.Char == '\n') { AddToken(); _rd.NextChar(); goto case 1; }
                    else if (Char.IsLetterOrDigit(_rd.Char)) { AddChar(); _rd.NextChar(); goto case 2; }
                    else { AddToken(); goto case 1; }
                    break;
                case 3:
                    if (_rd.Eof) { AddToken(); }
                    else if (_rd.Char == '\'') { AddToken(TokenKind.String); _rd.NextChar(); goto case 1; }
                    else { AddChar(); _rd.NextChar(); goto case 3; }
                    break;
                case 4:
                    if (_rd.Eof) { AddToken(TokenKind.IntegerNumber); }
                    else if (Char.IsDigit(_rd.Char)) { AddChar(); _rd.NextChar(); goto case 4; }
                    else if (_rd.Char == '.') { AddChar(); _rd.NextChar(); goto case 5; }
                    else { AddToken(TokenKind.IntegerNumber); goto case 1; }
                    break;
                case 5:
                    if (_rd.Eof) { AddToken(TokenKind.FloatNumber); }
                    else if (Char.IsDigit(_rd.Char)) { AddChar(); _rd.NextChar(); goto case 5; }
                    else { AddToken(TokenKind.FloatNumber); goto case 1; }
                    break;
                case 6:
                    if (_rd.Char == '=') { AddChar(); AddToken(); _rd.NextChar(); goto case 1; }
                    else { AddToken(); _rd.NextChar(); goto case 1; }
                case 7:
                    if (_rd.Char == '=') { AddChar(); AddToken(TokenKind.NotEqualsOperation); _rd.NextChar(); goto case 1; }
                    else { AddToken(); _rd.NextChar(); goto case 1; }
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

        private void AddToken(TokenKind? kind = null)
        {
            if (_valueBufferLength == 0) return;
            var value = new string(_valueBuffer, 0, _valueBufferLength);
            if (String.IsNullOrWhiteSpace(value)) throw new ArgumentException("Token value cannot be empty");
            _tokens.Add(new Token(value, kind ?? GetTokenKind(value), _row, _col));
            _valueBufferLength = 0;
        }

        private readonly Dictionary<string, TokenKind> _tokenKinds = new Dictionary<string, TokenKind>(StringComparer.OrdinalIgnoreCase)
        {
            { "SELECT", TokenKind.SelectKeyword },
            { "UPDATE", TokenKind.UpdateKeyword },
            { "DELETE", TokenKind.DeleteKeyword },
            { "SET", TokenKind.SetKeyword },
            { "FROM", TokenKind.FromKeyword },
            { "WHERE", TokenKind.WhereKeyword },
            { "ORDERBY", TokenKind.OrderByKeyword },
            { "SKIP", TokenKind.SkipKeyword },
            { "LIMIT", TokenKind.LimitKeyword },
            { "ASC", TokenKind.AscKeyword },
            { "DESC", TokenKind.DescKeyword },
            { "*", TokenKind.Asterisk },
            { "AND", TokenKind.AndOperation },
            { "OR", TokenKind.OrOperation },
            { "=", TokenKind.EqualsOperation },
            { "!=", TokenKind.NotEqualsOperation},
            { "<", TokenKind.LessOperation },
            { "<=", TokenKind.LessOrEqualsOperation },
            { ">", TokenKind.GreatOperation },
            { ">=", TokenKind.GreatOrEqualsOperation },
            { "LIKE", TokenKind.LikeOperation },
            { "IN", TokenKind.InOperation },
            { "(", TokenKind.OpenBracket },
            { ")", TokenKind.CloseBracket },
            { ",", TokenKind.Comma }
        };

        private TokenKind GetTokenKind(string tokenValue)
        {
            if (_tokenKinds.ContainsKey(tokenValue))
            {
                return _tokenKinds[tokenValue];
            }
            else
            {
                return TokenKind.Identificator;
            }
        }
    }
}
