namespace SimpleDB.Sql
{
    internal class Token
    {
        public readonly string Value;
        public readonly int Row;
        public readonly int Col;
        public readonly TokenKind Kind;

        public Token(string value, TokenKind kind, int row, int col)
        {
            Value = value;
            Row = row;
            Col = col;
            Kind = kind;
        }
    }

    internal enum TokenKind
    {
        SelectKeyword,
        FromKeyword,
        WhereKeyword,
        OrderByKeyword,
        SkipKeyword,
        LimitKeyword,
        AscKeyword,
        DescKeyword,
        Asterisk,
        AndOperation,
        OrOperation,
        EqualsOperation,
        NotEqualsOperation,
        GreatOperation,
        LessOperation,
        GreatOrEqualsOperation,
        LessOrEqualsOperation,
        LikeOperation,
        InOperation,
        Identificator,
        IntegerNumber,
        FloatNumber,
        String,
        OpenBracket,
        CloseBracket,
        Comma
    }
}
