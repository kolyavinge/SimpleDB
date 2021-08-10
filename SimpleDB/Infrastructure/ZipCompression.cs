using System;
using System.IO;
using System.IO.Compression;

namespace SimpleDB.Infrastructure
{
    internal static class ZipCompression
    {
        public static byte[] Compress(byte[] source)
        {
            using (var outputStream = new MemoryStream())
            {
                using (var zipStream = new GZipStream(outputStream, CompressionMode.Compress))
                using (var inputStream = new MemoryStream(source)) inputStream.CopyTo(zipStream);
                return outputStream.ToArray();
            }
        }

        public static byte[] Decompress(byte[] compressed)
        {
            using (var inputStream = new MemoryStream(compressed))
            using (var zipStream = new GZipStream(inputStream, CompressionMode.Decompress))
            using (var outputStream = new MemoryStream())
            {
                zipStream.CopyTo(outputStream);
                return outputStream.ToArray();
            }
        }
    }
}
