using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Core;

namespace SimpleDB.Queries
{
    internal class OrderByClause : IComparer<FieldValueCollection>
    {
        public OrderByClause(IEnumerable<OrderByClauseItem> orderedItems)
        {
            OrderedItems = orderedItems;
        }

        public IEnumerable<OrderByClauseItem> OrderedItems { get; }

        public int Compare(FieldValueCollection x, FieldValueCollection y)
        {
            foreach (var orderedItem in OrderedItems)
            {
                IComparable? xComparable = null;
                object? yComparable = null;
                if (orderedItem is Field)
                {
                    var orderedField = (Field)orderedItem;
                    xComparable = (IComparable?)x[orderedField.Number].Value;
                    yComparable = y[orderedField.Number].Value;
                }
                else if (orderedItem is PrimaryKey)
                {
                    xComparable = (IComparable)x.PrimaryKey.Value;
                    yComparable = y.PrimaryKey.Value;
                }
                int compareResult = 0;
                if (xComparable != null)
                {
                    compareResult = xComparable.CompareTo(yComparable);
                }
                else if (yComparable is IComparable)
                {
                    compareResult = ((IComparable)yComparable).CompareTo(xComparable);
                }
                if (compareResult == 0) continue;
                if (orderedItem.Direction == SortDirection.Desc) compareResult = -compareResult;

                return compareResult;
            }

            return 0;
        }

        public IEnumerable<byte> GetAllFieldNumbers()
        {
            var result = OrderedItems.OfType<Field>().Select(x => x.Number).Distinct().ToList();
            if (OrderedItems.OfType<PrimaryKey>().Any())
            {
                result.Add(Core.PrimaryKey.FieldNumber);
            }

            return result;
        }

        public abstract class OrderByClauseItem
        {
            public SortDirection Direction { get; protected set; }
        }

        public class PrimaryKey : OrderByClauseItem
        {
            public PrimaryKey(SortDirection direction)
            {
                Direction = direction;
            }
        }

        public class Field : OrderByClauseItem
        {
            public Field(byte number, SortDirection direction)
            {
                Number = number;
                Direction = direction;
            }

            public byte Number { get; }
        }
    }
}
