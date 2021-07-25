using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using SimpleDB.Infrastructure;

[assembly: InternalsVisibleTo("SimpleDB.Test")]

namespace SimpleDB.Core
{
    internal class DataFile
    {
        private readonly string _fileFullPath;
        private readonly Dictionary<byte, FieldMeta> _fieldMetaDictionary;
        private readonly IFileStream _fileStream;

        public DataFile(string fileFullPath, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            _fileFullPath = fileFullPath;
            _fieldMetaDictionary = fieldMetaCollection.ToDictionary(k => k.Number, v => v);
            IOC.Get<IFileSystem>().CreateFileIfNeeded(_fileFullPath);
            _fileStream = IOC.Get<IFileSystem>().OpenFile(_fileFullPath);
        }

        public InsertResult Insert(IEnumerable<FieldValue> fieldValueCollection)
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.End);
            var startDataFileOffset = _fileStream.Position;
            foreach (var fieldValue in fieldValueCollection)
            {
                _fileStream.Write(fieldValue.Number);
                var fieldMeta = _fieldMetaDictionary[fieldValue.Number];
                if (fieldMeta.Type == typeof(bool))
                {
                    _fileStream.Write((bool)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(sbyte))
                {
                    _fileStream.Write((sbyte)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(byte))
                {
                    _fileStream.Write((byte)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(char))
                {
                    _fileStream.Write((char)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(short))
                {
                    _fileStream.Write((short)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(ushort))
                {
                    _fileStream.Write((ushort)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(int))
                {
                    _fileStream.Write((int)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(uint))
                {
                    _fileStream.Write((uint)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(long))
                {
                    _fileStream.Write((long)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(ulong))
                {
                    _fileStream.Write((ulong)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(float))
                {
                    _fileStream.Write((float)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(double))
                {
                    _fileStream.Write((double)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(decimal))
                {
                    _fileStream.Write((decimal)fieldValue.Value);
                }
                else if (fieldMeta.Type == typeof(string))
                {
                    _fileStream.Write((string)fieldValue.Value);
                }
                else
                {
                    var fieldValueJson = JsonSerialization.ToJson(fieldValue.Value);
                    _fileStream.Write(fieldValueJson);
                }
            }

            return new InsertResult { StartDataFileOffset = startDataFileOffset, EndDataFileOffset = _fileStream.Position };
        }

        public IEnumerable<FieldValue> ReadFields(long startDataFileOffset, long endDataFileOffset)
        {
            _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            while (_fileStream.Position < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                if (_fieldMetaDictionary.ContainsKey(fieldNumber))
                {
                    object fieldValue;
                    var fieldMeta = _fieldMetaDictionary[fieldNumber];
                    if (fieldMeta.Type == typeof(bool))
                    {
                        fieldValue = _fileStream.ReadBool();
                    }
                    else if (fieldMeta.Type == typeof(sbyte))
                    {
                        fieldValue = _fileStream.ReadSByte();
                    }
                    else if (fieldMeta.Type == typeof(byte))
                    {
                        fieldValue = _fileStream.ReadByte();
                    }
                    else if (fieldMeta.Type == typeof(char))
                    {
                        fieldValue = _fileStream.ReadChar();
                    }
                    else if (fieldMeta.Type == typeof(short))
                    {
                        fieldValue = _fileStream.ReadShort();
                    }
                    else if (fieldMeta.Type == typeof(ushort))
                    {
                        fieldValue = _fileStream.ReadUShort();
                    }
                    else if (fieldMeta.Type == typeof(int))
                    {
                        fieldValue = _fileStream.ReadInt();
                    }
                    else if (fieldMeta.Type == typeof(uint))
                    {
                        fieldValue = _fileStream.ReadUInt();
                    }
                    else if (fieldMeta.Type == typeof(long))
                    {
                        fieldValue = _fileStream.ReadLong();
                    }
                    else if (fieldMeta.Type == typeof(ulong))
                    {
                        fieldValue = _fileStream.ReadULong();
                    }
                    else if (fieldMeta.Type == typeof(float))
                    {
                        fieldValue = _fileStream.ReadFloat();
                    }
                    else if (fieldMeta.Type == typeof(double))
                    {
                        fieldValue = _fileStream.ReadDouble();
                    }
                    else if (fieldMeta.Type == typeof(decimal))
                    {
                        fieldValue = _fileStream.ReadDecimal();
                    }
                    else if (fieldMeta.Type == typeof(string))
                    {
                        fieldValue = _fileStream.ReadString();
                    }
                    else
                    {
                        var fieldValueJson = _fileStream.ReadString();
                        fieldValue = JsonSerialization.FromJson(fieldMeta.Type, fieldValueJson);
                    }
                    yield return new FieldValue(fieldNumber, fieldValue);
                }
            }
        }

        public struct InsertResult
        {
            public long StartDataFileOffset;
            public long EndDataFileOffset;
        }
    }

    internal static class DataFileFileName
    {
        public static string FromCollectionName(string collectionName)
        {
            return String.Format("{0}.data", collectionName);
        }
    }
}
