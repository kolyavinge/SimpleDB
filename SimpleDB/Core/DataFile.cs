using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SimpleDB.Infrastructure;

namespace SimpleDB.Core
{
    internal class DataFile
    {
        private readonly IFileSystem _fileSystem;
        private readonly Dictionary<byte, FieldMeta> _fieldMetaDictionary;
        private readonly IMemoryBuffer _memoryBuffer;
        private IFileStream? _fileStream;

        public string FileName { get; }

        public DataFile(string fileName, IEnumerable<FieldMeta> fieldMetaCollection, IFileSystem fileSystem, IMemory memory)
        {
            FileName = fileName;
            _fileSystem = fileSystem;
            _fieldMetaDictionary = fieldMetaCollection.ToDictionary(k => k.Number, v => v);
            _memoryBuffer = memory.GetBuffer();
        }

        public long SizeInBytes => _fileStream!.Length;

        public void BeginRead()
        {
            _fileStream = _fileSystem.OpenFileRead(FileName);
        }

        public void BeginReadWrite()
        {
            _fileStream = _fileSystem.OpenFileReadWrite(FileName);
        }

        public void EndReadWrite()
        {
            _fileStream!.Dispose();
        }

        public InsertResult Insert(IEnumerable<FieldValue> fieldValueCollection)
        {
            var startDataFileOffset = _fileStream!.Seek(0, System.IO.SeekOrigin.End);
            _memoryBuffer.Seek(0, System.IO.SeekOrigin.Begin);
            InsertValues(_memoryBuffer, fieldValueCollection, out int insertedBytesCount);
            _fileStream.WriteByteArray(_memoryBuffer.BufferArray, 0, insertedBytesCount);
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
                if (fieldValueDictionary.TryGetValue(fieldMeta.Number, out var fieldValue))
                {
                    InsertValue(stream, fieldMeta, fieldValue, out insertedBytesCount);
                }
                else
                {
                    InsertValue(stream, fieldMeta, fieldMeta.GetDefaultValue(), out insertedBytesCount);
                }
                totalInsertedBytesCount += insertedBytesCount;
            }
            insertedBytesCount = totalInsertedBytesCount;
        }

        private void InsertValue(IWriteableStream stream, FieldMeta fieldMeta, object? fieldValue, out int insertedBytesCount)
        {
            stream.WriteByte((byte)FieldTypesConverter.GetFieldType(fieldMeta.Type));
            insertedBytesCount = sizeof(byte);
            if (fieldMeta.Type == typeof(bool))
            {
                stream.WriteBool((bool)fieldValue!);
                insertedBytesCount += sizeof(bool);
            }
            else if (fieldMeta.Type == typeof(sbyte))
            {
                stream.WriteSByte((sbyte)fieldValue!);
                insertedBytesCount += sizeof(sbyte);
            }
            else if (fieldMeta.Type == typeof(byte))
            {
                stream.WriteByte((byte)fieldValue!);
                insertedBytesCount += sizeof(byte);
            }
            else if (fieldMeta.Type == typeof(char))
            {
                stream.WriteUShort((char)fieldValue!); // char хранится как ushort
                insertedBytesCount += sizeof(ushort);
            }
            else if (fieldMeta.Type == typeof(short))
            {
                stream.WriteShort((short)fieldValue!);
                insertedBytesCount += sizeof(short);
            }
            else if (fieldMeta.Type == typeof(ushort))
            {
                stream.WriteUShort((ushort)fieldValue!);
                insertedBytesCount += sizeof(ushort);
            }
            else if (fieldMeta.Type == typeof(int))
            {
                stream.WriteInt((int)fieldValue!);
                insertedBytesCount += sizeof(int);
            }
            else if (fieldMeta.Type == typeof(uint))
            {
                stream.WriteUInt((uint)fieldValue!);
                insertedBytesCount += sizeof(uint);
            }
            else if (fieldMeta.Type == typeof(long))
            {
                stream.WriteLong((long)fieldValue!);
                insertedBytesCount += sizeof(long);
            }
            else if (fieldMeta.Type == typeof(ulong))
            {
                stream.WriteULong((ulong)fieldValue!);
                insertedBytesCount += sizeof(ulong);
            }
            else if (fieldMeta.Type == typeof(float))
            {
                stream.WriteFloat((float)fieldValue!);
                insertedBytesCount += sizeof(float);
            }
            else if (fieldMeta.Type == typeof(double))
            {
                stream.WriteDouble((double)fieldValue!);
                insertedBytesCount += sizeof(double);
            }
            else if (fieldMeta.Type == typeof(decimal))
            {
                stream.WriteDecimal((decimal)fieldValue!);
                insertedBytesCount += sizeof(decimal);
            }
            else if (fieldMeta.Type == typeof(DateTime))
            {
                stream.WriteLong(((DateTime)fieldValue!).ToBinary());
                insertedBytesCount += sizeof(long);
            }
            else if (fieldMeta.Type == typeof(string))
            {
                if (fieldValue == null)
                {
                    stream.WriteInt(-1);
                    insertedBytesCount += sizeof(int);
                }
                else if (fieldValue is string fieldValueString)
                {
                    var bytes = StringToByteArray(fieldMeta, fieldValueString);
                    stream.WriteInt(bytes.Length);
                    stream.WriteByteArray(bytes, 0, bytes.Length);
                    insertedBytesCount += sizeof(int) + bytes.Length;
                }
                else if (fieldValue is byte[] bytes)
                {
                    stream.WriteInt(bytes.Length);
                    stream.WriteByteArray(bytes, 0, bytes.Length);
                    insertedBytesCount += sizeof(int) + bytes.Length;
                }
            }
            else if (fieldValue is byte[] fieldValueBytes)
            {
                if (fieldValue == null)
                {
                    stream.WriteInt(-1);
                    insertedBytesCount += sizeof(int);
                }
                else
                {
                    var bytes = ToByteArray(fieldMeta, fieldValueBytes);
                    stream.WriteInt(bytes.Length);
                    stream.WriteByteArray(bytes, 0, bytes.Length);
                    insertedBytesCount += sizeof(int) + bytes.Length;
                }
            }
            else // object
            {
                byte[] bytes;
                if (fieldValue is byte[])
                {
                    bytes = (byte[])fieldValue;
                }
                else if (fieldValue is ObjectContainer)
                {
                    bytes = StringToByteArray(fieldMeta, fieldValue.ToString());
                }
                else
                {
                    bytes = ObjectToByteArray(fieldMeta, fieldValue!);
                }
                stream.WriteInt(bytes.Length);
                stream.WriteByteArray(bytes, 0, bytes.Length);
                insertedBytesCount += sizeof(int) + bytes.Length;
            }
        }

        private byte[] StringToByteArray(FieldMeta fieldMeta, string fieldValue)
        {
            var bytes = Encoding.UTF8.GetBytes(fieldValue);
            if (fieldMeta.Settings.Compressed)
            {
                bytes = ZipCompression.Compress(bytes);
            }

            return bytes;
        }

        private byte[] ToByteArray(FieldMeta fieldMeta, byte[] fieldValue)
        {
            if (fieldMeta.Settings.Compressed)
            {
                fieldValue = ZipCompression.Compress(fieldValue);
            }

            return fieldValue;
        }

        private byte[] ObjectToByteArray(FieldMeta fieldMeta, object fieldValue)
        {
            var fieldValueJson = JsonSerialization.ToJson(fieldValue);
            var bytes = Encoding.UTF8.GetBytes(fieldValueJson);
            if (fieldMeta.Settings.Compressed)
            {
                bytes = ZipCompression.Compress(bytes);
            }

            return bytes;
        }

        public byte[] ToByteArray(byte fieldNumber, object fieldValue)
        {
            var fieldMeta = _fieldMetaDictionary[fieldNumber];
            if (fieldMeta.Type == typeof(string))
            {
                return StringToByteArray(fieldMeta, (string)fieldValue);
            }
            else if (fieldMeta.Type == typeof(byte[]))
            {
                return ToByteArray(fieldMeta, (byte[])fieldValue);
            }
            else
            {
                return ObjectToByteArray(fieldMeta, fieldValue);
            }
        }

        public UpdateResult Update(long startDataFileOffset, long endDataFileOffset, IEnumerable<FieldValue> fieldValueCollection)
        {
            _memoryBuffer.Seek(0, System.IO.SeekOrigin.Begin);
            InsertValues(_memoryBuffer, fieldValueCollection, out int newLength);
            if (newLength <= endDataFileOffset - startDataFileOffset)
            {
                if (_fileStream!.Position != startDataFileOffset)
                {
                    _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
                }
                _fileStream.WriteByteArray(_memoryBuffer.BufferArray, 0, newLength);
                return new UpdateResult { NewStartDataFileOffset = startDataFileOffset, NewEndDataFileOffset = startDataFileOffset + newLength };
            }
            else
            {
                var newStartDataFileOffset = _fileStream!.Seek(0, System.IO.SeekOrigin.End);
                _fileStream.WriteByteArray(_memoryBuffer.BufferArray, 0, newLength);
                return new UpdateResult { NewStartDataFileOffset = newStartDataFileOffset, NewEndDataFileOffset = newStartDataFileOffset + newLength };
            }
        }

        public void UpdateManual(long startDataFileOffset, long endDataFileOffset, IEnumerable<FieldValue> fieldValueCollection)
        {
            var fieldValueDictionary = fieldValueCollection.ToDictionary(k => k.Number, v => v);
            var currentPosition = _fileStream!.Position;
            if (currentPosition != startDataFileOffset)
            {
                currentPosition = _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            }
            while (currentPosition < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                currentPosition += sizeof(byte);
                if (fieldValueDictionary.ContainsKey(fieldNumber))
                {
                    var fieldMeta = _fieldMetaDictionary[fieldNumber];
                    var fieldValue = fieldValueDictionary[fieldNumber].Value;
                    InsertValue(_fileStream, fieldMeta, fieldValue, out int insertedBytesCount);
                    currentPosition += insertedBytesCount;
                }
                else
                {
                    var fieldType = (FieldTypes)_fileStream.ReadByte();
                    currentPosition += sizeof(byte) + SkipCurrentField(fieldType);
                }
            }
        }

        public void ReadFieldsLength(long startDataFileOffset, long endDataFileOffset, ISet<byte> fieldNumbers, Dictionary<byte, int> result)
        {
            var currentPosition = _fileStream!.Position;
            if (currentPosition != startDataFileOffset)
            {
                currentPosition = _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            }
            while (currentPosition < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                var fieldType = (FieldTypes)_fileStream.ReadByte();
                var fieldLength = SkipCurrentField(fieldType);
                if (_fieldMetaDictionary.ContainsKey(fieldNumber) && fieldNumbers.Contains(fieldNumber))
                {
                    result.Add(fieldNumber, fieldLength);
                }
                currentPosition += 2 * sizeof(byte) + fieldLength;
            }
        }

        private readonly byte[] _skipBuffer = new byte[1024 * 1024];
        public void ReadFields(long startDataFileOffset, long endDataFileOffset, ISet<byte> fieldNumbers, FieldValueCollection result)
        {
            var currentPosition = _fileStream!.Position;
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
                    object? fieldValue = ReadValue(_fileStream, fieldMeta, out int readedBytesCount);
                    result.Add(new FieldValue(fieldNumber, fieldValue));
                    currentPosition += readedBytesCount;
                }
                else
                {
                    var fieldType = (FieldTypes)_fileStream.ReadByte();
                    currentPosition += sizeof(byte);
                    currentPosition += SkipCurrentField(fieldType);
                }
            }
        }

        public int GetUnusedFieldsSize(long startDataFileOffset, long endDataFileOffset, ISet<byte> fieldNumbers)
        {
            var unusedFieldsSize = 0;
            var currentPosition = _fileStream!.Position;
            if (currentPosition != startDataFileOffset)
            {
                currentPosition = _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            }
            while (currentPosition < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                var fieldType = (FieldTypes)_fileStream.ReadByte();
                var fieldReadBytes = 2 * sizeof(byte);
                fieldReadBytes += SkipCurrentField(fieldType);
                if (!_fieldMetaDictionary.ContainsKey(fieldNumber) || !fieldNumbers.Contains(fieldNumber))
                {
                    unusedFieldsSize += fieldReadBytes;
                }
                currentPosition += fieldReadBytes;
            }

            return unusedFieldsSize;
        }

        private int SkipCurrentField(FieldTypes fieldType)
        {
            int skippedBytes = 0;
            if (fieldType == FieldTypes.String || fieldType == FieldTypes.ByteArray || fieldType == FieldTypes.Object)
            {
                var length = _fileStream!.ReadInt();
                skippedBytes += sizeof(int);
                if (length > 0)
                {
                    _fileStream.ReadByteArray(_skipBuffer, 0, length);
                    skippedBytes += length;
                }
            }
            else
            {
                var fieldTypeSize = FieldTypesSize.GetSize(fieldType);
                _fileStream!.ReadByteArray(_skipBuffer, 0, fieldTypeSize);
                skippedBytes += fieldTypeSize;
            }

            return skippedBytes;
        }

        private static object? ReadValue(IReadableStream stream, FieldMeta fieldMeta, out int readedBytesCount)
        {
            readedBytesCount = 0;
            stream.ReadByte(); // skip 'fieldType'
            readedBytesCount += sizeof(byte);
            object? fieldValue;
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
                fieldValue = (char)stream.ReadUShort(); // char хранится как ushort
                readedBytesCount += sizeof(ushort);
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
                    if (fieldMeta.Settings.Compressed)
                    {
                        bytes = ZipCompression.Decompress(bytes);
                    }
                    fieldValue = Encoding.UTF8.GetString(bytes);
                    readedBytesCount += length;
                }
            }
            else if (fieldMeta.Type == typeof(byte[]))
            {
                var length = stream.ReadInt();
                readedBytesCount += sizeof(int);
                var bytes = stream.ReadByteArray(length);
                if (fieldMeta.Settings.Compressed)
                {
                    bytes = ZipCompression.Decompress(bytes);
                }
                fieldValue = bytes;
                readedBytesCount += length;
            }
            else
            {
                var length = stream.ReadInt();
                readedBytesCount += sizeof(int) + length;
                var bytes = stream.ReadByteArray(length);
                if (fieldMeta.Settings.Compressed)
                {
                    bytes = ZipCompression.Decompress(bytes);
                }
                var fieldValueJson = Encoding.UTF8.GetString(bytes);
                if (fieldMeta.Type != null)
                {
                    fieldValue = JsonSerialization.FromJson(fieldMeta.Type, fieldValueJson);
                }
                else
                {
                    fieldValue = new ObjectContainer(fieldValueJson);
                }
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

    internal interface IDataFileFactory
    {
        DataFile MakeFromFileName(string fileName, IEnumerable<FieldMeta> fieldMetaCollection);
        DataFile MakeFromEntityName(string entityName, IEnumerable<FieldMeta> fieldMetaCollection);
    }

    internal class DataFileFactory : IDataFileFactory
    {
        private readonly IFileSystem _fileSystem;
        private readonly IMemory _memory;

        public DataFileFactory(IFileSystem fileSystem, IMemory? memory = null)
        {
            _fileSystem = fileSystem;
            _memory = memory ?? Memory.Instance;
        }

        public DataFile MakeFromFileName(string fileName, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            return new DataFile(fileName, fieldMetaCollection, _fileSystem, _memory);
        }

        public DataFile MakeFromEntityName(string entityName, IEnumerable<FieldMeta> fieldMetaCollection)
        {
            return MakeFromFileName(DataFileName.FromEntityName(entityName), fieldMetaCollection);
        }
    }

    internal static class DataFileName
    {
        public const string Extension = ".data";

        public static string FromEntityName(string entityName)
        {
            return $"{entityName}{Extension}";
        }
    }
}
