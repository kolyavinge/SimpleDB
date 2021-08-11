using System;
using SimpleDB.Utils.ObjectExtension;

namespace SimpleDB.Queries
{
    internal static class SmartComparer
    {
        public static bool AreEquals(object x, object y)
        {
            if (x.GetType() != y.GetType() && x.IsStandartType() && y.IsStandartType())
            {
                if (x.GetStandartTypeSize() > y.GetStandartTypeSize())
                {
                    if (x.IsUnsignedType() && y.IsNegative())
                    {
                        return false;
                    }
                    else
                    {
                        y = Convert.ChangeType(y, x.GetType());
                    }
                }
                else
                {
                    if (y.IsUnsignedType() && x.IsNegative())
                    {
                        return false;
                    }
                    else
                    {
                        x = Convert.ChangeType(x, y.GetType());
                    }
                }
            }

            return x.Equals(y);
        }

        public static int Compare(object x, object y)
        {
            if (x.GetType() != y.GetType() && x.IsStandartType() && y.IsStandartType())
            {
                if (x.GetStandartTypeSize() > y.GetStandartTypeSize())
                {
                    if (x.IsUnsignedType() && y.IsNegative())
                    {
                        return -1;
                    }
                    else
                    {
                        y = Convert.ChangeType(y, x.GetType());
                    }
                }
                else
                {
                    if (y.IsUnsignedType() && x.IsNegative())
                    {
                        return 1;
                    }
                    else
                    {
                        x = Convert.ChangeType(x, y.GetType());
                    }
                }
            }

            var xComparable = (IComparable)x;
            var yComparable = (IComparable)y;

            return xComparable.CompareTo(yComparable);
        }
    }
}
