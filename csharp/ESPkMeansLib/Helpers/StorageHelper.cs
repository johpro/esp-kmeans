using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ESPkMeansLib.Helpers
{
    public static class StorageHelper
    {
        public static BinaryWriter GetWriter(string fn, bool useGzipCompression = false)
        {
            Stream s = new FileStream(fn, FileMode.Create);
            if (useGzipCompression)
                s = new BufferedStream(new GZipStream(s, CompressionLevel.Optimal));
            return new BinaryWriter(s);
        }

        public static BinaryReader GetReader(string fn, bool decompressGzip = false)
        {
            Stream s = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            if (decompressGzip)
                s = new BufferedStream(new GZipStream(s, CompressionMode.Decompress));
            return new BinaryReader(s);
        }
    }
}
