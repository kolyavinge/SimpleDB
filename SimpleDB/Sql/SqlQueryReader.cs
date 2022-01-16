namespace SimpleDB.Sql
{
    internal class SqlQueryReader
    {
        private readonly string _sqlQuery;
        private int _index;

        public int Row { get; private set; }
        public int Col { get; private set; }
        public char Char { get; private set; }
        public bool Eof => _index == _sqlQuery.Length;

        public SqlQueryReader(string sqlQuery)
        {
            _sqlQuery = sqlQuery;
            _index = -1;
            Col = -1;
        }

        public void NextChar()
        {
            if (Eof) return;
            _index++;
            if (Eof) return;
            Char = _sqlQuery[_index];
            if (Char == '\n')
            {
                Row++;
                Col = -1;
            }
            else if (Char == '\r')
            {
                NextChar();
            }
            else
            {
                Col++;
            }
        }
    }
}
