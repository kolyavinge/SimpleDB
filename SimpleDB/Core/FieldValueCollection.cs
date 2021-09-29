﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;

namespace SimpleDB.Core
{
    internal class FieldValueCollection : IEnumerable<FieldValue>, IEquatable<FieldValue>
    {
        private readonly Dictionary<byte, FieldValue> _fieldValues;
        private int? _hashCode;

        public FieldValueCollection()
        {
            _fieldValues = new Dictionary<byte, FieldValue>();
        }

        public FieldValueCollection(IEnumerable<FieldValue> fieldValues)
        {
            _fieldValues = fieldValues.ToDictionary(k => k.Number, v => v);
        }

        public PrimaryKey PrimaryKey { get; set; }

        public bool Contains(byte fieldNumber)
        {
            return _fieldValues.ContainsKey(fieldNumber);
        }

        public FieldValue this[byte fieldNumber]
        {
            get { return _fieldValues[fieldNumber]; }
            set
            {
                _fieldValues[fieldNumber] = value;
                _hashCode = null;
            }
        }

        public void Add(byte fieldNumber, FieldValue fieldValue)
        {
            _fieldValues.Add(fieldNumber, fieldValue);
            _hashCode = null;
        }

        public int Count
        {
            get { return _fieldValues.Count; }
        }

        public void Clear()
        {
            _fieldValues.Clear();
        }

        public IEnumerator<FieldValue> GetEnumerator()
        {
            return _fieldValues.Values.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return _fieldValues.Values.GetEnumerator();
        }

        public bool Equals(FieldValue x)
        {
            return Equals((object)x);
        }

        public override bool Equals(object obj)
        {
            var x = obj as FieldValueCollection;
            if (x == null) return false;
            if (Count != x.Count) return false;
            foreach (var fieldValue in this)
            {
                if (!x.Contains(fieldValue.Number)) return false;
                if (!x[fieldValue.Number].Value.Equals(fieldValue.Value)) return false;
            }

            return true;
        }

        public override int GetHashCode()
        {
            if (_hashCode == null)
            {
                unchecked
                {
                    _hashCode = 1430287;
                    foreach (var kv in _fieldValues)
                    {
                        _hashCode *= 7302013 ^ kv.Value.Value.GetHashCode();
                    }
                }
            }

            return _hashCode.Value;
        }

        public FieldValueCollection Merge(FieldValueCollection x)
        {
            foreach (var xitem in x)
            {
                if (!Contains(xitem.Number))
                {
                    Add(xitem.Number, xitem);
                }
            }

            return this;
        }

        public static IEnumerable<FieldValueCollection> Intersect(IEnumerable<FieldValueCollection> x, IEnumerable<FieldValueCollection> y)
        {
            x = x ?? Enumerable.Empty<FieldValueCollection>();
            y = y ?? Enumerable.Empty<FieldValueCollection>();
            return from xitem in x
                   join yitem in y
                   on xitem.PrimaryKey.Value equals yitem.PrimaryKey.Value
                   select xitem.Merge(yitem);
        }

        public static IEnumerable<FieldValueCollection> Union(IEnumerable<FieldValueCollection> x, IEnumerable<FieldValueCollection> y)
        {
            x = x ?? Enumerable.Empty<FieldValueCollection>();
            y = y ?? Enumerable.Empty<FieldValueCollection>();
            var result = x.ToDictionary(k => k.PrimaryKey.Value, v => v);
            foreach (var yitem in y)
            {
                if (result.ContainsKey(yitem.PrimaryKey.Value))
                {
                    result[yitem.PrimaryKey.Value].Merge(yitem);
                }
                else
                {
                    result.Add(yitem.PrimaryKey.Value, yitem);
                }
            }

            return result.Values;
        }

        public static IEnumerable<FieldValueCollection> Merge(IEnumerable<FieldValueCollection> x, IEnumerable<FieldValueCollection> y)
        {
            x = x ?? Enumerable.Empty<FieldValueCollection>();
            y = y ?? Enumerable.Empty<FieldValueCollection>();
            var result = x.ToDictionary(k => k.PrimaryKey.Value, v => v);
            foreach (var yitem in y)
            {
                if (result.ContainsKey(yitem.PrimaryKey.Value))
                {
                    result[yitem.PrimaryKey.Value].Merge(yitem);
                }
            }

            return result.Values;
        }
    }
}
