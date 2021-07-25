using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using SimpleDB.Infrastructure;

[assembly: InternalsVisibleTo("SimpleDB.Test")]

namespace SimpleDB.Core
{
    internal class PrimaryKeyFile
    {
        private readonly string _fileFullPath;
        private readonly Type _primaryKeyType;
        private readonly IFileStream _fileStream;

        public PrimaryKeyFile(string fileFullPath, Type primaryKeyType)
        {
            _fileFullPath = fileFullPath;
            _primaryKeyType = primaryKeyType;
            IOC.Get<IFileSystem>().CreateFileIfNeeded(_fileFullPath);
            _fileStream = IOC.Get<IFileSystem>().OpenFile(_fileFullPath);
        }

        public void Insert(PrimaryKey primaryKey)
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.End);
            if (_primaryKeyType == typeof(sbyte))
            {
                _fileStream.Write((sbyte)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(byte))
            {
                _fileStream.Write((byte)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(char))
            {
                _fileStream.Write((char)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(short))
            {
                _fileStream.Write((short)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(ushort))
            {
                _fileStream.Write((ushort)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(int))
            {
                _fileStream.Write((int)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(uint))
            {
                _fileStream.Write((uint)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(long))
            {
                _fileStream.Write((long)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(ulong))
            {
                _fileStream.Write((ulong)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(float))
            {
                _fileStream.Write((float)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(double))
            {
                _fileStream.Write((double)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(decimal))
            {
                _fileStream.Write((decimal)primaryKey.Value);
            }
            else if (_primaryKeyType == typeof(string))
            {
                _fileStream.Write((string)primaryKey.Value);
            }
            else
            {
                var primaryKeyValueJson = JsonSerialization.ToJson(primaryKey.Value);
                _fileStream.Write(primaryKeyValueJson);
            }
            _fileStream.Write(primaryKey.StartDataFileOffset);
            _fileStream.Write(primaryKey.EndDataFileOffset);
        }

        public IEnumerable<PrimaryKey> GetAllPrimaryKeys()
        {
            _fileStream.Seek(0, System.IO.SeekOrigin.Begin);
            while (_fileStream.EOF == false)
            {
                object primaryKeyValue;
                if (_primaryKeyType == typeof(sbyte))
                {
                    primaryKeyValue = _fileStream.ReadSByte();
                }
                else if (_primaryKeyType == typeof(byte))
                {
                    primaryKeyValue = _fileStream.ReadByte();
                }
                else if (_primaryKeyType == typeof(char))
                {
                    primaryKeyValue = _fileStream.ReadChar();
                }
                else if (_primaryKeyType == typeof(short))
                {
                    primaryKeyValue = _fileStream.ReadShort();
                }
                else if (_primaryKeyType == typeof(ushort))
                {
                    primaryKeyValue = _fileStream.ReadUShort();
                }
                else if (_primaryKeyType == typeof(int))
                {
                    primaryKeyValue = _fileStream.ReadInt();
                }
                else if (_primaryKeyType == typeof(uint))
                {
                    primaryKeyValue = _fileStream.ReadUInt();
                }
                else if (_primaryKeyType == typeof(long))
                {
                    primaryKeyValue = _fileStream.ReadLong();
                }
                else if (_primaryKeyType == typeof(ulong))
                {
                    primaryKeyValue = _fileStream.ReadULong();
                }
                else if (_primaryKeyType == typeof(float))
                {
                    primaryKeyValue = _fileStream.ReadFloat();
                }
                else if (_primaryKeyType == typeof(double))
                {
                    primaryKeyValue = _fileStream.ReadDouble();
                }
                else if (_primaryKeyType == typeof(decimal))
                {
                    primaryKeyValue = _fileStream.ReadDecimal();
                }
                else if (_primaryKeyType == typeof(string))
                {
                    primaryKeyValue = _fileStream.ReadString();
                }
                else
                {
                    var primaryKeyValueJson = _fileStream.ReadString();
                    primaryKeyValue = JsonSerialization.FromJson(_primaryKeyType, primaryKeyValueJson);
                }
                var startDataFileOffset = _fileStream.ReadLong();
                var endDataFileOffset = _fileStream.ReadLong();
                yield return new PrimaryKey(primaryKeyValue, startDataFileOffset, endDataFileOffset);
            }
        }
    }

    internal static class PrimaryKeyFileName
    {
        public static string FromCollectionName(string collectionName)
        {
            return String.Format("{0}.primary", collectionName);
        }
    }
}
