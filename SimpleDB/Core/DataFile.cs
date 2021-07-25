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
            foreach (var fieldValue in fieldValueCollection)
            {
                stream.Write(fieldValue.Number);
                var fieldMeta = _fieldMetaDictionary[fieldValue.Number];
                InsertValue(stream, fieldMeta, fieldValue);
            }
        }

        private void InsertValue(IWriteableStream stream, FieldMeta fieldMeta, FieldValue fieldValue)
        {
            if (fieldMeta.Type == typeof(bool))
            {
                stream.Write((bool)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(sbyte))
            {
                stream.Write((sbyte)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(byte))
            {
                stream.Write((byte)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(char))
            {
                stream.Write((char)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(short))
            {
                stream.Write((short)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(ushort))
            {
                stream.Write((ushort)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(int))
            {
                stream.Write((int)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(uint))
            {
                stream.Write((uint)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(long))
            {
                stream.Write((long)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(ulong))
            {
                stream.Write((ulong)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(float))
            {
                stream.Write((float)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(double))
            {
                stream.Write((double)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(decimal))
            {
                stream.Write((decimal)fieldValue.Value);
            }
            else if (fieldMeta.Type == typeof(string))
            {
                stream.Write((string)fieldValue.Value);
            }
            else
            {
                var fieldValueJson = JsonSerialization.ToJson(fieldValue.Value);
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

        public IEnumerable<FieldValue> ReadFields(long startDataFileOffset, long endDataFileOffset)
        {
            _fileStream.Seek(startDataFileOffset, System.IO.SeekOrigin.Begin);
            while (_fileStream.Position < endDataFileOffset)
            {
                var fieldNumber = _fileStream.ReadByte();
                if (_fieldMetaDictionary.ContainsKey(fieldNumber))
                {
                    var fieldMeta = _fieldMetaDictionary[fieldNumber];
                    object fieldValue = ReadValue(_fileStream, fieldMeta);

                    yield return new FieldValue(fieldNumber, fieldValue);
                }
            }
        }

        private object ReadValue(IReadableStream stream, FieldMeta fieldMeta)
        {
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
                fieldValue = stream.ReadString();
            }
            else
            {
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
