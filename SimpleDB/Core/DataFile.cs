using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class DataFile : IDisposable
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

        public void Dispose()
        {
            _fileStream.Flush();
            _fileStream.Dispose();
        }

        public InsertResult Insert(IEnumerable<FieldValue> fieldValueCollection)
        {
            var startDataFileOffset = _fileStream.Seek(0, System.IO.SeekOrigin.End);
            int insertedBytesCount;
            InsertValues(_fileStream, fieldValueCollection, out insertedBytesCount);
            var endDataFileOffset = startDataFileOffset + insertedBytesCount;

            return new InsertResult { StartDataFileOffset = startDataFileOffset, EndDataFileOffset = endDataFileOffset };
        }

        private void InsertValues(IWriteableStream stream, IEnumerable<FieldValue> fieldValueCollection, out int insertedBytesCount)
        {
            int totalInsertedBytesCount = 0;
            var fieldValueDictionary = fieldValueCollection.ToDictionary(k => k.Number, v => v.Value);
            foreach (var fieldMeta in _fieldMetaDictionary.Values)
            {
                stream.WriteByte(fieldMeta.Number);
                totalInsertedBytesCount += sizeof(byte);
                if (fieldValueDictionary.ContainsKey(fieldMeta.Number))
                {
                    InsertValue(stream, fieldMeta, fieldValueDictionary[fieldMeta.Number], out insertedBytesCount);
                }
                else
                {
                    InsertValue(stream, fieldMeta, fieldMeta.GetDefaultValue(), out insertedBytesCount);
                }
                totalInsertedBytesCount += insertedBytesCount;
            }
            insertedBytesCount = totalInsertedBytesCount;
        }

        private static void InsertValue(IWriteableStream stream, FieldMeta fieldMeta, object fieldValue, out int insertedBytesCount)
        {
            stream.WriteByte((byte)fieldMeta.GetFieldType());
            insertedBytesCount = sizeof(byte);
            if (fieldMeta.Type == typeof(bool))
            {
                stream.WriteBool((bool)fieldValue);
                insertedBytesCount += sizeof(bool);
            }
            else if (fieldMeta.Type == typeof(sbyte))
            {
                stream.WriteSByte((sbyte)fieldValue);
                insertedBytesCount += sizeof(sbyte);
            }
            else if (fieldMeta.Type == typeof(byte))
            {
                stream.WriteByte((byte)fieldValue);
                insertedBytesCount += sizeof(byte);
            }
            else if (fieldMeta.Type == typeof(char))
            {
                stream.WriteChar((char)fieldValue);
                insertedBytesCount += sizeof(char);
            }
            else if (fieldMeta.Type == typeof(short))
            {
                stream.WriteShort((short)fieldValue);
                insertedBytesCount += sizeof(short);
            }
            else if (fieldMeta.Type == typeof(ushort))
            {
                stream.WriteUShort((ushort)fieldValue);
                insertedBytesCount += sizeof(ushort);
            }
            else if (fieldMeta.Type == typeof(int))
            {
                stream.WriteInt((int)fieldValue);
                insertedBytesCount += sizeof(int);
            }
            else if (fieldMeta.Type == typeof(uint))
            {
                stream.WriteUInt((uint)fieldValue);
                insertedBytesCount += sizeof(uint);
            }
            else if (fieldMeta.Type == typeof(long))
            {
                stream.WriteLong((long)fieldValue);
                insertedBytesCount += sizeof(long);
            }
            else if (fieldMeta.Type == typeof(ulong))
            {
                stream.WriteULong((ulong)fieldValue);
                insertedBytesCount += sizeof(ulong);
            }
            else if (fieldMeta.Type == typeof(float))
            {
                stream.WriteFloat((float)fieldValue);
                insertedBytesCount += sizeof(float);
            }
            else if (fieldMeta.Type == typeof(double))
            {
                stream.WriteDouble((double)fieldValue);
                insertedBytesCount += sizeof(double);
            }
            else if (fieldMeta.Type == typeof(decimal))
            {
                stream.WriteDecimal((decimal)fieldValue);
                insertedBytesCount += sizeof(decimal);
            }
            else if (fieldMeta.Type == typeof(DateTime))
            {
                stream.WriteLong(((DateTime)fieldValue).ToBinary());
                insertedBytesCount += sizeof(long);
            }
            else if (fieldMeta.Type == typeof(string))
            {
                var str = (string)fieldValue;
                if (str == null)
                {
                    stream.WriteInt(-1);
                    insertedBytesCount += sizeof(int);
                }
                else
                {
                    var bytes = Encoding.UTF8.GetBytes(str);
                    stream.WriteInt(bytes.Length);
                    stream.WriteByteArray(bytes, 0, bytes.Length);
                    insertedBytesCount += sizeof(int) + bytes.Length;
                }
            }
            else
            {
                var fieldValueJson = JsonSerialization.ToJson(fieldValue);
                var strBytes = Encoding.UTF8.GetBytes(fieldValueJson);
                stream.WriteInt(strBytes.Length);
                stream.WriteByteArray(strBytes, 0, strBytes.Length);
                insertedBytesCount += sizeof(int) + strBytes.Length;
            }
        }

        public UpdateResult Update(long startDataFileOffset, long endDataFileOffset, IEnumerable<FieldValue> fieldValueCollection)
        {
            _memoryBuffer.Seek(0, System.IO.SeekOrigin.Begin);
            int newLength;
            InsertValues(_memoryBuffer, fieldValueCollection, out newLength);
            if (newLength <= endDataFileOffset - startDataFileOffset)
            {
                if (_fileStream.Position != startDataFileOffset)
                {
                    _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
                }
                _fileStream.WriteByteArray(_memoryBuffer.BufferArray, 0, newLength);
                return new UpdateResult { NewStartDataFileOffset = startDataFileOffset, NewEndDataFileOffset = startDataFileOffset + newLength };
            }
            else
            {
                var newStartDataFileOffset = _fileStream.Seek(0, System.IO.SeekOrigin.End);
                _fileStream.WriteByteArray(_memoryBuffer.BufferArray, 0, newLength);
                return new UpdateResult { NewStartDataFileOffset = newStartDataFileOffset, NewEndDataFileOffset = newStartDataFileOffset + newLength };
            }
        }

        private readonly byte[] _skipBuffer = new byte[1024 * 1024];
        public void ReadFields(long startDataFileOffset, long endDataFileOffset, ISet<byte> fieldNumbers, Dictionary<byte, FieldValue> result)
        {
            var currentPosition = _fileStream.Position;
            if (currentPosition != startDataFileOffset)
            {
                currentPosition = _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            }
            while (currentPosition < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                currentPosition += sizeof(byte);
                if (_fieldMetaDictionary.ContainsKey(fieldNumber) && fieldNumbers.Contains(fieldNumber))
                {
                    var fieldMeta = _fieldMetaDictionary[fieldNumber];
                    int readedBytesCount;
                    object fieldValue = ReadValue(_fileStream, fieldMeta, out readedBytesCount);
                    result.Add(fieldNumber, new FieldValue(fieldNumber, fieldValue));
                    currentPosition += readedBytesCount;
                }
                else
                {
                    var fieldType = (FieldTypes)_fileStream.ReadByte();
                    currentPosition += sizeof(byte);
                    if (fieldType == FieldTypes.String || fieldType == FieldTypes.Object)
                    {
                        var length = _fileStream.ReadInt();
                        currentPosition += sizeof(int);
                        if (length > 0)
                        {
                            _fileStream.ReadByteArray(_skipBuffer, 0, length);
                            currentPosition += length;
                        }
                    }
                    else
                    {
                        var fieldTypeSize = FieldTypesSize.GetSize(fieldType);
                        _fileStream.ReadByteArray(_skipBuffer, 0, fieldTypeSize);
                        currentPosition += fieldTypeSize;
                    }
                }
            }
        }

        private static object ReadValue(IReadableStream stream, FieldMeta fieldMeta, out int readedBytesCount)
        {
            readedBytesCount = 0;
            stream.ReadByte(); // skip 'fieldType'
            readedBytesCount += sizeof(byte);
            object fieldValue;
            if (fieldMeta.Type == typeof(bool))
            {
                fieldValue = stream.ReadBool();
                readedBytesCount += sizeof(bool);
            }
            else if (fieldMeta.Type == typeof(sbyte))
            {
                fieldValue = stream.ReadSByte();
                readedBytesCount += sizeof(sbyte);
            }
            else if (fieldMeta.Type == typeof(byte))
            {
                fieldValue = stream.ReadByte();
                readedBytesCount += sizeof(byte);
            }
            else if (fieldMeta.Type == typeof(char))
            {
                fieldValue = stream.ReadChar();
                readedBytesCount += sizeof(char);
            }
            else if (fieldMeta.Type == typeof(short))
            {
                fieldValue = stream.ReadShort();
                readedBytesCount += sizeof(short);
            }
            else if (fieldMeta.Type == typeof(ushort))
            {
                fieldValue = stream.ReadUShort();
                readedBytesCount += sizeof(ushort);
            }
            else if (fieldMeta.Type == typeof(int))
            {
                fieldValue = stream.ReadInt();
                readedBytesCount += sizeof(int);
            }
            else if (fieldMeta.Type == typeof(uint))
            {
                fieldValue = stream.ReadUInt();
                readedBytesCount += sizeof(uint);
            }
            else if (fieldMeta.Type == typeof(long))
            {
                fieldValue = stream.ReadLong();
                readedBytesCount += sizeof(long);
            }
            else if (fieldMeta.Type == typeof(ulong))
            {
                fieldValue = stream.ReadULong();
                readedBytesCount += sizeof(ulong);
            }
            else if (fieldMeta.Type == typeof(float))
            {
                fieldValue = stream.ReadFloat();
                readedBytesCount += sizeof(float);
            }
            else if (fieldMeta.Type == typeof(double))
            {
                fieldValue = stream.ReadDouble();
                readedBytesCount += sizeof(double);
            }
            else if (fieldMeta.Type == typeof(decimal))
            {
                fieldValue = stream.ReadDecimal();
                readedBytesCount += sizeof(decimal);
            }
            else if (fieldMeta.Type == typeof(DateTime))
            {
                var dateTimeLong = stream.ReadLong();
                fieldValue = DateTime.FromBinary(dateTimeLong);
                readedBytesCount += sizeof(long);
            }
            else if (fieldMeta.Type == typeof(string))
            {
                var length = stream.ReadInt();
                readedBytesCount += sizeof(int);
                if (length == -1)
                {
                    fieldValue = null;
                }
                else
                {
                    var bytes = stream.ReadByteArray(length);
                    fieldValue = Encoding.UTF8.GetString(bytes);
                    readedBytesCount += length;
                }
            }
            else
            {
                var length = stream.ReadInt();
                readedBytesCount += sizeof(int) + length;
                var bytes = stream.ReadByteArray(length);
                var fieldValueJson = Encoding.UTF8.GetString(bytes);
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
