using System;
using System.Collections.Generic;

namespace SimpleDB.Core;

internal class FieldMeta
{
    public byte Number { get; protected set; }

    public string Name { get; }

    public Type Type { get; }

    public FieldSettings Settings { get; set; }

    public FieldMeta(byte number, string name, Type type) : this(name, type)
    {
        if (number == 0) throw new ArgumentException("Number must be greater than zero");
        Number = number;
    }

    protected FieldMeta(string name, Type type)
    {
        Name = name;
        Type = type;
    }

    public object? GetDefaultValue()
    {
        if (Type == typeof(bool)) return default(bool);
        if (Type == typeof(sbyte)) return default(sbyte);
        if (Type == typeof(byte)) return default(byte);
        if (Type == typeof(char)) return default(char);
        if (Type == typeof(short)) return default(short);
        if (Type == typeof(ushort)) return default(ushort);
        if (Type == typeof(int)) return default(int);
        if (Type == typeof(uint)) return default(uint);
        if (Type == typeof(long)) return default(long);
        if (Type == typeof(ulong)) return default(ulong);
        if (Type == typeof(float)) return default(float);
        if (Type == typeof(double)) return default(double);
        if (Type == typeof(decimal)) return default(decimal);
        if (Type == typeof(DateTime)) return default(DateTime);

        return null;
    }

    public override bool Equals(object obj)
    {
        return obj is FieldMeta meta &&
               Number == meta.Number &&
               EqualityComparer<Type>.Default.Equals(Type, meta.Type) &&
               EqualityComparer<FieldSettings>.Default.Equals(Settings, meta.Settings);
    }

    public override int GetHashCode()
    {
        unchecked
        {
            int hash = (int)2166136261;
            hash = (hash * 16777619) ^ Number.GetHashCode();
            hash = (hash * 16777619) ^ Type.GetHashCode();
            hash = (hash * 16777619) ^ Settings.GetHashCode();
            return hash;
        }
    }
}

internal class PrimaryKeyFieldMeta : FieldMeta
{
    public PrimaryKeyFieldMeta(string name, Type type) :
        base(name, type)
    {
        Number = PrimaryKey.FieldNumber;
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
    ByteArray = 16,
    Object = 255
}

internal static class FieldTypesSize
{
    public static int GetSize(FieldTypes type)
    {
        if (type == FieldTypes.Bool) return sizeof(bool);
        if (type == FieldTypes.Sbyte) return sizeof(sbyte);
        if (type == FieldTypes.Byte) return sizeof(byte);
        if (type == FieldTypes.Char) return sizeof(ushort); // char хранится как ushort
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

        throw new UnsupportedFieldTypeException();
    }
}

internal class FieldTypesConverter
{
    public static Type GetType(FieldTypes type)
    {
        if (type == FieldTypes.Bool) return typeof(bool);
        if (type == FieldTypes.Sbyte) return typeof(sbyte);
        if (type == FieldTypes.Byte) return typeof(byte);
        if (type == FieldTypes.Char) return typeof(char);
        if (type == FieldTypes.Short) return typeof(short);
        if (type == FieldTypes.UShort) return typeof(ushort);
        if (type == FieldTypes.Int) return typeof(int);
        if (type == FieldTypes.UInt) return typeof(uint);
        if (type == FieldTypes.Long) return typeof(long);
        if (type == FieldTypes.ULong) return typeof(ulong);
        if (type == FieldTypes.Float) return typeof(float);
        if (type == FieldTypes.Double) return typeof(double);
        if (type == FieldTypes.Decimal) return typeof(decimal);
        if (type == FieldTypes.DateTime) return typeof(DateTime);
        if (type == FieldTypes.String) return typeof(string);
        if (type == FieldTypes.ByteArray) return typeof(byte[]);

        throw new UnsupportedFieldTypeException();
    }

    public static FieldTypes GetFieldType(Type type)
    {
        if (type == typeof(bool)) return FieldTypes.Bool;
        if (type == typeof(sbyte)) return FieldTypes.Sbyte;
        if (type == typeof(byte)) return FieldTypes.Byte;
        if (type == typeof(char)) return FieldTypes.Char;
        if (type == typeof(short)) return FieldTypes.Short;
        if (type == typeof(ushort)) return FieldTypes.UShort;
        if (type == typeof(int)) return FieldTypes.Int;
        if (type == typeof(uint)) return FieldTypes.UInt;
        if (type == typeof(long)) return FieldTypes.Long;
        if (type == typeof(ulong)) return FieldTypes.ULong;
        if (type == typeof(float)) return FieldTypes.Float;
        if (type == typeof(double)) return FieldTypes.Double;
        if (type == typeof(decimal)) return FieldTypes.Decimal;
        if (type == typeof(DateTime)) return FieldTypes.DateTime;
        if (type == typeof(string)) return FieldTypes.String;
        if (type == typeof(byte[])) return FieldTypes.ByteArray;

        return FieldTypes.Object;
    }
}

internal class UnsupportedFieldTypeException : Exception { }
