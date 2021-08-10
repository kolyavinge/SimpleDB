using System;

namespace SimpleDB.Core
{
    internal class FieldMeta
    {
        public byte Number { get; private set; }

        public Type Type { get; private set; }

        public FieldSettings Settings { get; set; }

        public FieldMeta(byte number, Type type)
        {
            Number = number;
            Type = type;
        }

        public FieldTypes GetFieldType()
        {
            if (Type == typeof(bool)) return FieldTypes.Bool;
            else if (Type == typeof(sbyte)) return FieldTypes.Sbyte;
            else if (Type == typeof(byte)) return FieldTypes.Byte;
            else if (Type == typeof(char)) return FieldTypes.Char;
            else if (Type == typeof(short)) return FieldTypes.Short;
            else if (Type == typeof(ushort)) return FieldTypes.UShort;
            else if (Type == typeof(int)) return FieldTypes.Int;
            else if (Type == typeof(uint)) return FieldTypes.UInt;
            else if (Type == typeof(long)) return FieldTypes.Long;
            else if (Type == typeof(ulong)) return FieldTypes.ULong;
            else if (Type == typeof(float)) return FieldTypes.Float;
            else if (Type == typeof(double)) return FieldTypes.Double;
            else if (Type == typeof(decimal)) return FieldTypes.Decimal;
            else if (Type == typeof(DateTime)) return FieldTypes.DateTime;
            else if (Type == typeof(string)) return FieldTypes.String;
            else return FieldTypes.Object;
        }

        public object GetDefaultValue()
        {
            if (Type == typeof(bool)) return default(bool);
            else if (Type == typeof(sbyte)) return default(sbyte);
            else if (Type == typeof(byte)) return default(byte);
            else if (Type == typeof(char)) return default(char);
            else if (Type == typeof(short)) return default(short);
            else if (Type == typeof(ushort)) return default(ushort);
            else if (Type == typeof(int)) return default(int);
            else if (Type == typeof(uint)) return default(uint);
            else if (Type == typeof(long)) return default(long);
            else if (Type == typeof(ulong)) return default(ulong);
            else if (Type == typeof(float)) return default(float);
            else if (Type == typeof(double)) return default(double);
            else if (Type == typeof(decimal)) return default(decimal);
            else if (Type == typeof(DateTime)) return default(DateTime);
            else return null;
        }
    }

    internal enum FieldTypes : byte
    {
        Bool = 1,
        Sbyte = 2,
        Byte = 3,
        Char = 4,
        Short = 5,
        UShort = 6,
        Int = 7,
        UInt = 8,
        Long = 9,
        ULong = 10,
        Float = 11,
        Double = 12,
        Decimal = 13,
        DateTime = 14,
        String = 15,
        Object = 255
    }

    internal static class FieldTypesSize
    {
        public static int GetSize(FieldTypes type)
        {
            if (type == FieldTypes.Bool) return sizeof(bool);
            if (type == FieldTypes.Sbyte) return sizeof(sbyte);
            if (type == FieldTypes.Byte) return sizeof(byte);
            if (type == FieldTypes.Char) return sizeof(char);
            if (type == FieldTypes.Short) return sizeof(short);
            if (type == FieldTypes.UShort) return sizeof(ushort);
            if (type == FieldTypes.Int) return sizeof(int);
            if (type == FieldTypes.UInt) return sizeof(uint);
            if (type == FieldTypes.Long) return sizeof(long);
            if (type == FieldTypes.ULong) return sizeof(ulong);
            if (type == FieldTypes.Float) return sizeof(float);
            if (type == FieldTypes.Double) return sizeof(double);
            if (type == FieldTypes.Decimal) return sizeof(decimal);
            if (type == FieldTypes.DateTime) return sizeof(long);
            else throw new UnsupportedFieldTypeException();
        }
    }

    internal class UnsupportedFieldTypeException : Exception { }
}
