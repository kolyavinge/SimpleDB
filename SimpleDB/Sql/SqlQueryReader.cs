namespace SimpleDB.Sql
{
    internal class SqlQueryReader
    {
        private readonly string _sqlQuery;
        private int _index;
        private int _start, _end;
        private int _row;
        private int _col;
        private int _startCol;
        private int _startRow;

        public char CurrentChar { get; private set; }

        public SqlQueryReaderValue Value
        {
            get
            {
                return new SqlQueryReaderValue
                {
                    Value = _sqlQuery.Substring(_start, _end - _start + 1),
                    Row = _startRow,
                    Col = _startCol
                };
            }
        }

        public bool Eof { get { return _index == _sqlQuery.Length - 1; } }

        public SqlQueryReader(string sqlQuery)
        {
            _sqlQuery = sqlQuery;
            CurrentChar = _sqlQuery[0];
        }

        public void NextChar()
        {
            if (Eof) return;
            _index++;
            CurrentChar = _sqlQuery[_index];
            if (CurrentChar == '\n')
            {
                _row++;
                _col = -1;
                NextChar();
            }
            else if (CurrentChar == '\r')
            {
                NextChar();
            }
            else
            {
                _col++;
            }
        }

        public void StartReadValue()
        {
            _start = _index;
            _startCol = _col;
            _startRow = _row;
        }

        public void EndReadValue()
        {
            _end = _index;
        }
    }

    internal struct SqlQueryReaderValue
    {
        public string Value;
        public int Row;
        public int Col;
    }
}
