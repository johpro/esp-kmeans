using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ESPkMeansLib.Tests.Helpers
{
    public static class FileHelper
    {

        public static StreamReader GetReader(string fn)
        {
            var isGzip = fn.EndsWith(".gz", StringComparison.OrdinalIgnoreCase);
            return new StreamReader(isGzip
                ? (Stream)new BufferedStream(new GZipStream(File.OpenRead(fn), CompressionMode.Decompress))
                : File.OpenRead(fn), bufferSize: 4096);
        }

        public static IEnumerable<string> ReadLines(string fn)
        {
            using (var reader = GetReader(fn))
            {
                string l;
                while ((l = reader.ReadLine()) != null)
                    yield return l;
            }
        }
    }
}
