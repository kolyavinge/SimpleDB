using System;
using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Core
{
    internal class IndexAdapter
    {
        private readonly AbstractIndex _index;

        public IndexAdapter(AbstractIndex index)
        {
            _index = index;
        }

        public IndexValue GetEquals(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetEquals((bool)value);
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetEquals((sbyte)value);
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetEquals((byte)value);
            else if (type == typeof(char)) return ((Index<char>)_index).GetEquals((char)value);
            else if (type == typeof(short)) return ((Index<short>)_index).GetEquals((short)value);
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetEquals((ushort)value);
            else if (type == typeof(int)) return ((Index<int>)_index).GetEquals((int)value);
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetEquals((uint)value);
            else if (type == typeof(long)) return ((Index<long>)_index).GetEquals((long)value);
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetEquals((ulong)value);
            else if (type == typeof(float)) return ((Index<float>)_index).GetEquals((float)value);
            else if (type == typeof(double)) return ((Index<double>)_index).GetEquals((double)value);
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetEquals((decimal)value);
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetEquals((DateTime)value);
            else if (type == typeof(string)) return ((Index<string>)_index).GetEquals((string)value);
            else return ((Index<IComparable<object>>)_index).GetEquals((IComparable<object>)value);
        }

        public IEnumerable<IndexValue> GetNotEquals(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetNotEquals((bool)value);
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetNotEquals((sbyte)value);
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetNotEquals((byte)value);
            else if (type == typeof(char)) return ((Index<char>)_index).GetNotEquals((char)value);
            else if (type == typeof(short)) return ((Index<short>)_index).GetNotEquals((short)value);
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetNotEquals((ushort)value);
            else if (type == typeof(int)) return ((Index<int>)_index).GetNotEquals((int)value);
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetNotEquals((uint)value);
            else if (type == typeof(long)) return ((Index<long>)_index).GetNotEquals((long)value);
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetNotEquals((ulong)value);
            else if (type == typeof(float)) return ((Index<float>)_index).GetNotEquals((float)value);
            else if (type == typeof(double)) return ((Index<double>)_index).GetNotEquals((double)value);
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetNotEquals((decimal)value);
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetNotEquals((DateTime)value);
            else if (type == typeof(string)) return ((Index<string>)_index).GetNotEquals((string)value);
            else return ((Index<IComparable<object>>)_index).GetNotEquals((IComparable<object>)value);
        }

        public IEnumerable<IndexValue> GetLess(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetLess((bool)value);
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetLess((sbyte)value);
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetLess((byte)value);
            else if (type == typeof(char)) return ((Index<char>)_index).GetLess((char)value);
            else if (type == typeof(short)) return ((Index<short>)_index).GetLess((short)value);
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetLess((ushort)value);
            else if (type == typeof(int)) return ((Index<int>)_index).GetLess((int)value);
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetLess((uint)value);
            else if (type == typeof(long)) return ((Index<long>)_index).GetLess((long)value);
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetLess((ulong)value);
            else if (type == typeof(float)) return ((Index<float>)_index).GetLess((float)value);
            else if (type == typeof(double)) return ((Index<double>)_index).GetLess((double)value);
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetLess((decimal)value);
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetLess((DateTime)value);
            else if (type == typeof(string)) return ((Index<string>)_index).GetLess((string)value);
            else return ((Index<IComparable<object>>)_index).GetLess((IComparable<object>)value);
        }

        public IEnumerable<IndexValue> GetGreat(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetGreat((bool)value);
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetGreat((sbyte)value);
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetGreat((byte)value);
            else if (type == typeof(char)) return ((Index<char>)_index).GetGreat((char)value);
            else if (type == typeof(short)) return ((Index<short>)_index).GetGreat((short)value);
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetGreat((ushort)value);
            else if (type == typeof(int)) return ((Index<int>)_index).GetGreat((int)value);
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetGreat((uint)value);
            else if (type == typeof(long)) return ((Index<long>)_index).GetGreat((long)value);
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetGreat((ulong)value);
            else if (type == typeof(float)) return ((Index<float>)_index).GetGreat((float)value);
            else if (type == typeof(double)) return ((Index<double>)_index).GetGreat((double)value);
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetGreat((decimal)value);
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetGreat((DateTime)value);
            else if (type == typeof(string)) return ((Index<string>)_index).GetGreat((string)value);
            else return ((Index<IComparable<object>>)_index).GetGreat((IComparable<object>)value);
        }

        public IEnumerable<IndexValue> GetLessOrEquals(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetLessOrEquals((bool)value);
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetLessOrEquals((sbyte)value);
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetLessOrEquals((byte)value);
            else if (type == typeof(char)) return ((Index<char>)_index).GetLessOrEquals((char)value);
            else if (type == typeof(short)) return ((Index<short>)_index).GetLessOrEquals((short)value);
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetLessOrEquals((ushort)value);
            else if (type == typeof(int)) return ((Index<int>)_index).GetLessOrEquals((int)value);
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetLessOrEquals((uint)value);
            else if (type == typeof(long)) return ((Index<long>)_index).GetLessOrEquals((long)value);
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetLessOrEquals((ulong)value);
            else if (type == typeof(float)) return ((Index<float>)_index).GetLessOrEquals((float)value);
            else if (type == typeof(double)) return ((Index<double>)_index).GetLessOrEquals((double)value);
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetLessOrEquals((decimal)value);
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetLessOrEquals((DateTime)value);
            else if (type == typeof(string)) return ((Index<string>)_index).GetLessOrEquals((string)value);
            else return ((Index<IComparable<object>>)_index).GetLessOrEquals((IComparable<object>)value);
        }

        public IEnumerable<IndexValue> GetGreatOrEquals(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetGreatOrEquals((bool)value);
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetGreatOrEquals((sbyte)value);
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetGreatOrEquals((byte)value);
            else if (type == typeof(char)) return ((Index<char>)_index).GetGreatOrEquals((char)value);
            else if (type == typeof(short)) return ((Index<short>)_index).GetGreatOrEquals((short)value);
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetGreatOrEquals((ushort)value);
            else if (type == typeof(int)) return ((Index<int>)_index).GetGreatOrEquals((int)value);
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetGreatOrEquals((uint)value);
            else if (type == typeof(long)) return ((Index<long>)_index).GetGreatOrEquals((long)value);
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetGreatOrEquals((ulong)value);
            else if (type == typeof(float)) return ((Index<float>)_index).GetGreatOrEquals((float)value);
            else if (type == typeof(double)) return ((Index<double>)_index).GetGreatOrEquals((double)value);
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetGreatOrEquals((decimal)value);
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetGreatOrEquals((DateTime)value);
            else if (type == typeof(string)) return ((Index<string>)_index).GetGreatOrEquals((string)value);
            else return ((Index<IComparable<object>>)_index).GetGreatOrEquals((IComparable<object>)value);
        }

        public IEnumerable<IndexValue> GetLike(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(string)) return ((Index<string>)_index).GetLike((string)value);
            else throw new InvalidOperationException();
        }

        public IEnumerable<IndexValue> GetNotLike(object value)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(string)) return ((Index<string>)_index).GetNotLike((string)value);
            else throw new InvalidOperationException();
        }

        public IEnumerable<IndexValue> GetIn(IEnumerable<object> values)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetIn(values.Cast<bool>());
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetIn(values.Cast<sbyte>());
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetIn(values.Cast<byte>());
            else if (type == typeof(char)) return ((Index<char>)_index).GetIn(values.Cast<char>());
            else if (type == typeof(short)) return ((Index<short>)_index).GetIn(values.Cast<short>());
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetIn(values.Cast<ushort>());
            else if (type == typeof(int)) return ((Index<int>)_index).GetIn(values.Cast<int>());
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetIn(values.Cast<uint>());
            else if (type == typeof(long)) return ((Index<long>)_index).GetIn(values.Cast<long>());
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetIn(values.Cast<ulong>());
            else if (type == typeof(float)) return ((Index<float>)_index).GetIn(values.Cast<float>());
            else if (type == typeof(double)) return ((Index<double>)_index).GetIn(values.Cast<double>());
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetIn(values.Cast<decimal>());
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetIn(values.Cast<DateTime>());
            else if (type == typeof(string)) return ((Index<string>)_index).GetIn(values.Cast<string>());
            else return ((Index<IComparable<object>>)_index).GetIn(values.Cast<IComparable<object>>());
        }

        public IEnumerable<IndexValue> GetNotIn(IEnumerable<object> values)
        {
            var type = _index.Meta.IndexedFieldType;
            if (type == typeof(bool)) return ((Index<bool>)_index).GetNotIn(values.Cast<bool>());
            else if (type == typeof(sbyte)) return ((Index<sbyte>)_index).GetNotIn(values.Cast<sbyte>());
            else if (type == typeof(byte)) return ((Index<byte>)_index).GetNotIn(values.Cast<byte>());
            else if (type == typeof(char)) return ((Index<char>)_index).GetNotIn(values.Cast<char>());
            else if (type == typeof(short)) return ((Index<short>)_index).GetNotIn(values.Cast<short>());
            else if (type == typeof(ushort)) return ((Index<ushort>)_index).GetNotIn(values.Cast<ushort>());
            else if (type == typeof(int)) return ((Index<int>)_index).GetNotIn(values.Cast<int>());
            else if (type == typeof(uint)) return ((Index<uint>)_index).GetNotIn(values.Cast<uint>());
            else if (type == typeof(long)) return ((Index<long>)_index).GetNotIn(values.Cast<long>());
            else if (type == typeof(ulong)) return ((Index<ulong>)_index).GetNotIn(values.Cast<ulong>());
            else if (type == typeof(float)) return ((Index<float>)_index).GetNotIn(values.Cast<float>());
            else if (type == typeof(double)) return ((Index<double>)_index).GetNotIn(values.Cast<double>());
            else if (type == typeof(decimal)) return ((Index<decimal>)_index).GetNotIn(values.Cast<decimal>());
            else if (type == typeof(DateTime)) return ((Index<DateTime>)_index).GetNotIn(values.Cast<DateTime>());
            else if (type == typeof(string)) return ((Index<string>)_index).GetNotIn(values.Cast<string>());
            else return ((Index<IComparable<object>>)_index).GetNotIn(values.Cast<IComparable<object>>());
        }
    }
}
