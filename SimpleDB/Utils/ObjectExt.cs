using System;

namespace SimpleDB.Utils.ObjectExtension
{
    internal static class ObjectExt
    {
        public static bool IsNegative(this object x)
        {
            return ((IComparable)x).CompareTo(Convert.ChangeType(0, x.GetType())) < 0;
        }

        public static bool IsStandartType(this object x)
        {
            return (x is byte) || (x is sbyte) || (x is short) || (x is ushort) || (x is int) || (x is uint) || (x is long) || (x is ulong) || (x is float) || (x is double) || (x is decimal);
        }

        public static int GetStandartTypeSize(this object x)
        {
            if (x is byte) return sizeof(byte);
            if (x is sbyte) return sizeof(sbyte);
            if (x is short) return sizeof(short);
            if (x is ushort) return sizeof(ushort);
            if (x is int) return sizeof(int);
            if (x is uint) return sizeof(uint);
            if (x is long) return sizeof(long);
            if (x is ulong) return sizeof(ulong);
            if (x is float) return sizeof(float);
            if (x is double) return sizeof(double);
            if (x is decimal) return sizeof(decimal);
            return 0;
        }

        public static bool IsSignedType(this object x)
        {
            return (x is sbyte) || (x is short) || (x is int) || (x is long) || (x is float) || (x is double) || (x is decimal);
        }

        public static bool IsUnsignedType(this object x)
        {
            return IsSignedType(x) == false;
        }
    }
}
