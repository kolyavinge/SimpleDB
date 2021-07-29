using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;

namespace SimpleDB.Queries
{
    internal class OrderByClause : IComparer<FieldValueDictionary>
    {
        public OrderByClause(IEnumerable<Field> orderedFields)
        {
            OrderedFields = orderedFields;
        }

        public IEnumerable<Field> OrderedFields { get; }

        public int Compare(FieldValueDictionary x, FieldValueDictionary y)
        {
            foreach (var orderedField in OrderedFields)
            {
                var xComparable = (IComparable)x.FieldValues[orderedField.Number].Value;
                var yComparable = (IComparable)y.FieldValues[orderedField.Number].Value;
                var compareResult = xComparable.CompareTo(yComparable);
                if (compareResult == 0) continue;
                if (orderedField.Direction == OrderByDirection.Desc) compareResult = -compareResult;
                return compareResult;
            }

            return 0;
        }

        public IEnumerable<byte> GetAllFieldNumbers()
        {
            return OrderedFields.Select(x => x.Number).Distinct();
        }

        public class Field
        {
            public Field(byte number, OrderByDirection direction)
            {
                Number = number;
                Direction = direction;
            }

            public byte Number { get; }
            public OrderByDirection Direction { get; }
        }

        public enum OrderByDirection { Asc, Desc }
    }
}
