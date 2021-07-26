using System;
using System.Collections.Generic;
using System.Linq;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class DataFile
    {
        private readonly string _fileFullPath;
        private readonly Dictionary<byte, FieldMeta> _fieldMetaDictionary;
        private readonly IFileStream _fileStream;
        private readonly IMemoryBuffer _memoryBuffer;

        public DataFile(string fileFullPath, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            _fileFullPath = fileFullPath;
            _fieldMetaDictionary = fieldMetaCollection.ToDictionary(k => k.Number, v => v);
            IOC.Get<IFileSystem>().CreateFileIfNeeded(_fileFullPath);
            _fileStream = IOC.Get<IFileSystem>().OpenFile(_fileFullPath);
            _memoryBuffer = IOC.Get<IMemory>().GetBuffer();
        }

        public InsertResult Insert(IEnumerable<FieldValue> fieldValueCollection)
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.End);
            var startDataFileOffset = _fileStream.Position;
            InsertValues(_fileStream, fieldValueCollection);

            return new InsertResult { StartDataFileOffset = startDataFileOffset, EndDataFileOffset = _fileStream.Position };
        }

        private void InsertValues(IWriteableStream stream, IEnumerable<FieldValue> fieldValueCollection)
        {
            foreach (var fieldMeta in _fieldMetaDictionary.Values)
            {
                stream.Write(fieldMeta.Number);
                var fieldValue = fieldValueCollection.FirstOrDefault(x => x.Number == fieldMeta.Number);
                if (fieldValue != null)
                {
                    InsertValue(stream, fieldMeta, fieldValue.Value);
                }
                else
                {
                    InsertValue(stream, fieldMeta, fieldMeta.GetDefaultValue());
                }
            }
        }

        private void InsertValue(IWriteableStream stream, FieldMeta fieldMeta, object fieldValue)
        {
            stream.Write((byte)fieldMeta.GetFieldType());
            if (fieldMeta.Type == typeof(bool))
            {
                stream.Write((bool)fieldValue);
            }
            else if (fieldMeta.Type == typeof(sbyte))
            {
                stream.Write((sbyte)fieldValue);
            }
            else if (fieldMeta.Type == typeof(byte))
            {
                stream.Write((byte)fieldValue);
            }
            else if (fieldMeta.Type == typeof(char))
            {
                stream.Write((char)fieldValue);
            }
            else if (fieldMeta.Type == typeof(short))
            {
                stream.Write((short)fieldValue);
            }
            else if (fieldMeta.Type == typeof(ushort))
            {
                stream.Write((ushort)fieldValue);
            }
            else if (fieldMeta.Type == typeof(int))
            {
                stream.Write((int)fieldValue);
            }
            else if (fieldMeta.Type == typeof(uint))
            {
                stream.Write((uint)fieldValue);
            }
            else if (fieldMeta.Type == typeof(long))
            {
                stream.Write((long)fieldValue);
            }
            else if (fieldMeta.Type == typeof(ulong))
            {
                stream.Write((ulong)fieldValue);
            }
            else if (fieldMeta.Type == typeof(float))
            {
                stream.Write((float)fieldValue);
            }
            else if (fieldMeta.Type == typeof(double))
            {
                stream.Write((double)fieldValue);
            }
            else if (fieldMeta.Type == typeof(decimal))
            {
                stream.Write((decimal)fieldValue);
            }
            else if (fieldMeta.Type == typeof(string))
            {
                var str = (string)fieldValue;
                if (String.IsNullOrWhiteSpace(str))
                {
                    stream.Write(-1);
                }
                else
                {
                    stream.Write(str.Length);
                    stream.Write(str);
                }
            }
            else
            {
                var fieldValueJson = JsonSerialization.ToJson(fieldValue);
                stream.Write(fieldValueJson.Length);
                stream.Write(fieldValueJson);
            }
        }

        public UpdateResult Update(long startDataFileOffset, long endDataFileOffset, IEnumerable<FieldValue> fieldValueCollection)
        {
            _memoryBuffer.Seek(0, System.IO.SeekOrigin.Begin);
            InsertValues(_memoryBuffer, fieldValueCollection);
            var newLength = _memoryBuffer.Position;
            if (newLength <= endDataFileOffset - startDataFileOffset)
            {
                _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
                _fileStream.Write(_memoryBuffer.BufferArray, 0, (int)newLength);
                return new UpdateResult { NewStartDataFileOffset = startDataFileOffset, NewEndDataFileOffset = _fileStream.Position };
            }
            else
            {
                _fileStream.Seek(0, System.IO.SeekOrigin.End);
                var newStartDataFileOffset = _fileStream.Position;
                _fileStream.Write(_memoryBuffer.BufferArray, 0, (int)newLength);
                return new UpdateResult { NewStartDataFileOffset = newStartDataFileOffset, NewEndDataFileOffset = _fileStream.Position };
            }
        }

        public void ReadFields(long startDataFileOffset, long endDataFileOffset, FieldValue[] result)
        {
            int resultIndex = 0;
            _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            while (_fileStream.Position < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                if (_fieldMetaDictionary.ContainsKey(fieldNumber))
                {
                    var fieldMeta = _fieldMetaDictionary[fieldNumber];
                    object fieldValue = ReadValue(_fileStream, fieldMeta);
                    result[resultIndex++] = new FieldValue(fieldNumber, fieldValue);
                }
            }
        }

        public void ReadFields(long startDataFileOffset, long endDataFileOffset, ISet<byte> fieldNumbers, FieldValue[] result)
        {
            int resultIndex = 0;
            _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            while (_fileStream.Position < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                if (_fieldMetaDictionary.ContainsKey(fieldNumber) && fieldNumbers.Contains(fieldNumber))
                {
                    var fieldMeta = _fieldMetaDictionary[fieldNumber];
                    object fieldValue = ReadValue(_fileStream, fieldMeta);
                    result[resultIndex++] = new FieldValue(fieldNumber, fieldValue);
                }
                else
                {
                    var fieldType = (FieldTypes)_fileStream.ReadByte();
                    if (fieldType == FieldTypes.String || fieldType == FieldTypes.Object)
                    {
                        var length = _fileStream.ReadInt();
                        if (length > 0)
                        {
                            _fileStream.Seek(length, System.IO.SeekOrigin.Current);
                        }
                    }
                    else
                    {
                        var fieldTypeSize = FieldTypesSize.GetSize(fieldType);
                        _fileStream.Seek(fieldTypeSize, System.IO.SeekOrigin.Current);
                    }
                }
            }
        }

        private object ReadValue(IReadableStream stream, FieldMeta fieldMeta)
        {
            stream.Seek(sizeof(byte), System.IO.SeekOrigin.Current); // skip 'fieldType'
            object fieldValue;
            if (fieldMeta.Type == typeof(bool))
            {
                fieldValue = stream.ReadBool();
            }
            else if (fieldMeta.Type == typeof(sbyte))
            {
                fieldValue = stream.ReadSByte();
            }
            else if (fieldMeta.Type == typeof(byte))
            {
                fieldValue = stream.ReadByte();
            }
            else if (fieldMeta.Type == typeof(char))
            {
                fieldValue = stream.ReadChar();
            }
            else if (fieldMeta.Type == typeof(short))
            {
                fieldValue = stream.ReadShort();
            }
            else if (fieldMeta.Type == typeof(ushort))
            {
                fieldValue = stream.ReadUShort();
            }
            else if (fieldMeta.Type == typeof(int))
            {
                fieldValue = stream.ReadInt();
            }
            else if (fieldMeta.Type == typeof(uint))
            {
                fieldValue = stream.ReadUInt();
            }
            else if (fieldMeta.Type == typeof(long))
            {
                fieldValue = stream.ReadLong();
            }
            else if (fieldMeta.Type == typeof(ulong))
            {
                fieldValue = stream.ReadULong();
            }
            else if (fieldMeta.Type == typeof(float))
            {
                fieldValue = stream.ReadFloat();
            }
            else if (fieldMeta.Type == typeof(double))
            {
                fieldValue = stream.ReadDouble();
            }
            else if (fieldMeta.Type == typeof(decimal))
            {
                fieldValue = stream.ReadDecimal();
            }
            else if (fieldMeta.Type == typeof(string))
            {
                var length = stream.ReadInt();
                if (length == -1)
                {
                    fieldValue = null;
                }
                else
                {
                    fieldValue = stream.ReadString();
                }
            }
            else
            {
                stream.Seek(sizeof(int), System.IO.SeekOrigin.Current); // skip 'length'
                var fieldValueJson = stream.ReadString();
                fieldValue = JsonSerialization.FromJson(fieldMeta.Type, fieldValueJson);
            }

            return fieldValue;
        }

        public struct InsertResult
        {
            public long StartDataFileOffset;
            public long EndDataFileOffset;
        }

        public struct UpdateResult
        {
            public long NewStartDataFileOffset;
            public long NewEndDataFileOffset;
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
