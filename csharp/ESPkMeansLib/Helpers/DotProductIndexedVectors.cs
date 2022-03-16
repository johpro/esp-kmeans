/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO.Compression;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text.Json;
using ESPkMeansLib.Model;

namespace ESPkMeansLib.Helpers
{
    /// <summary>
    /// Indexing structure to quickly retrieve indexed vectors in descending order of their dot product with query vector.
    /// Please note that this indexing structure was designed for dot products greater than zero, which is typical for
    /// text representations that only have positive or zero vector entries, for instance.
    /// That is, it is not possible to retrieve vectors hat have negative dot products.
    /// </summary>
    public class DotProductIndexedVectors
    {

        public int VectorsCount { get; private set; }
        public int MaxId { get; private set; }

        private readonly DictList<int, (int id, float tokenVal)> _tokenToVectorsMap = new();
        private readonly Dictionary<int, FlexibleVector> _indexedVectors = new();
        private readonly ConcurrentBag<Dictionary<int, float>> _intFloatDictionaryBag = new();


        public DotProductIndexedVectors()
        {

        }

        private DotProductIndexedVectors(StorageMeta meta, Dictionary<int, FlexibleVector> indexedVectors,
            DictList<int, (int id, float tokenVal)> globalMap)
        {
            _tokenToVectorsMap = globalMap;
            _indexedVectors = indexedVectors;
            VectorsCount = meta.VectorsCount;
            MaxId = meta.MaxId;
        }


        /// <summary>
        /// Clear the index
        /// </summary>
        public void Clear()
        {
            _indexedVectors.Clear();
            _tokenToVectorsMap.Clear();
            VectorsCount = 0;
            MaxId = 0;
        }


        /// <summary>
        /// Clear the index and add all provided vectors to the index, using the position in the array as identifier
        /// </summary>
        /// <param name="vectors">The vectors to add</param>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public void Set(FlexibleVector[] vectors)
        {
            if (vectors.Length == 0)
                return;
            Clear();
            _indexedVectors.EnsureCapacity(vectors.Length);
            for (var i = 0; i < vectors.Length; i++)
            {
                Add(vectors[i], i);
            }
        }

        /// <summary>
        /// Add specified vector to the index. A unique identifier has to be provided as well, which will be returned on querying the index.
        /// </summary>
        /// <param name="v">Vector to add</param>
        /// <param name="id">Unique number to identify vector</param>
        /// <exception cref="ArgumentException">will be thrown if number of vector entries is zero</exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public void Add(FlexibleVector v, int id)
        {
            if (!v.IsSparse)
                v = v.ToSparse();
            if (v.Length < 1)
                throw new ArgumentException("cannot add zero vector to index");
            VectorsCount++;
            MaxId = Math.Max(MaxId, id);
            _indexedVectors.Add(id, v);

            var indexes = v.Indexes;
            var values = v.Values;

            for (int i = 0; i < indexes.Length; i++)
            {
                _tokenToVectorsMap.AddToList(indexes[i], (id, values[i]));
            }
        }


        /// <summary>
        /// Retrieve corresponding vector from index
        /// </summary>
        /// <param name="id">identifier of the vector</param>
        /// <returns></returns>
        public FlexibleVector GetVectorById(int id)
        {
            return _indexedVectors[id];
        }
        /// <summary>
        /// Get the id and the corresponding dot product of the nearest vector.
        /// Returns id of -1 if no match has been found (with dot product > 0).
        /// Neighbor must have dot product > 0 due to the inner workings of this indexing structure.
        /// </summary>
        /// <param name="vector"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int id, float dotProduct) GetNearestVector(FlexibleVector vector)
        {
            return
                vector.Indexes.Length == 1
                    ? GetNearestVector(vector.Indexes[0], vector.Values[0])
                    : GetNearestVector(vector, out _);
        }


        internal (int id, float dotProduct) GetNearestVector(int index, float tokenVal)
        {
            if (!_tokenToVectorsMap.TryGetValue(index, out var list) || list.Count == 0)
                return (-1, default);
            var bestId = list[0].id;
            var bestVal = list[0].tokenVal;
            if (tokenVal >= 0)
            {
                for (int i = 1; i < list.Count; i++)
                {
                    var curVal = list[i].tokenVal;
                    if (curVal <= bestVal)
                        continue;
                    bestVal = curVal;
                    bestId = list[i].id;
                }
            }
            else
            {
                for (int i = 1; i < list.Count; i++)
                {
                    var curVal = list[i].tokenVal;
                    if (curVal >= bestVal)
                        continue;
                    bestVal = curVal;
                    bestId = list[i].id;
                }
            }

            var dp = tokenVal * bestVal;
            return dp > 0 ? (bestId, dp) : (-1, default);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        internal (int id, float dotProduct) GetNearestVector(FlexibleVector vector, out int affectedClustersCount)
        {
            if (!vector.IsSparse)
                vector = vector.ToSparse();
            var indexes = vector.Indexes;
            var values = vector.Values;
            if (_intFloatDictionaryBag.TryTake(out var dict))
                dict.Clear();
            else
                dict = new Dictionary<int, float>();
            for (int i = 0; i < indexes.Length; i++)
            {
                if (!_tokenToVectorsMap.TryGetValue(indexes[i], out var list))
                    continue;
                var val = values[i];
                for (int j = 0; j < list.Count; j++)
                {
                    var (id, tokenVal) = list[j];
                    dict.AddToItem(id, tokenVal * val);
                }
            }

            affectedClustersCount = dict.Count;

            if (dict.Count == 0)
            {
                _intFloatDictionaryBag.Add(dict);
                return (-1, default);
            }

            int bestId = -1;
            float bestDp = float.MinValue;


            foreach (var p in dict)
            {
                if (p.Value > bestDp)
                {
                    bestId = p.Key;
                    bestDp = p.Value;
                }
            }
            _intFloatDictionaryBag.Add(dict);
            if (bestDp <= 0)
                return (-1, default);
            return (bestId, bestDp);
        }
        

        /// <summary>
        /// Get nearest vector given that we indexed an array of vectors, so that we can use
        /// an array instead of a dictionary for accumulating the dot products.
        /// </summary>
        /// <param name="vector"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        internal unsafe (int id, float dotProduct) GetNearestVectorArrayDict(FlexibleVector vector)
        {
            if (!vector.IsSparse)
                throw new Exception("vector has to be sparse");
            var indexes = vector.Indexes;
            var values = vector.Values;
            var dict = ArrayPool<float>.Shared.Rent(MaxId + 1);
            const int vecSize = 8;
            if (Avx.IsSupported && vecSize != Vector256<float>.Count)
                throw new Exception("AVX vector size mismatch");
            Array.Clear(dict, 0, MaxId + 1);
            try
            {
                fixed (float* dictPtr = dict)
                {
                    for (int i = 0; i < indexes.Length; i++)
                    {
                        if (!_tokenToVectorsMap.TryGetValue(indexes[i], out var list))
                            continue;
                        var val = values[i];
                        var j = 0;
                        if (Avx.IsSupported && list.Count >= vecSize)
                        {
                            var limit = list.Count - vecSize;
                            var valVec = Vector256.Create(val);
                            for (; j <= limit; j += vecSize)
                            {
                                var (id7, tokenVal7) = list[j+7];
                                var (id0, tokenVal0) = list[j];
                                var (id1, tokenVal1) = list[j+1];
                                var (id2, tokenVal2) = list[j+2];
                                var (id3, tokenVal3) = list[j+3];
                                var (id4, tokenVal4) = list[j+4];
                                var (id5, tokenVal5) = list[j+5];
                                var (id6, tokenVal6) = list[j+6];

                                var tokenValVec = Vector256.Create(tokenVal0, tokenVal1, tokenVal2, tokenVal3,
                                    tokenVal4, tokenVal5, tokenVal6, tokenVal7);
                                var existingSums = Vector256.Create(dictPtr[id0], dictPtr[id1], dictPtr[id2],
                                    dictPtr[id3], dictPtr[id4], dictPtr[id5], dictPtr[id6], dictPtr[id7]);
                                
                                if (Fma.IsSupported)
                                {
                                    existingSums = Fma.MultiplyAdd(valVec, tokenValVec, existingSums);
                                }
                                else
                                {
                                    var mul = Avx.Multiply(valVec, tokenValVec);
                                    existingSums = Avx.Add(existingSums, mul);
                                }

                                dictPtr[id0] = existingSums.GetElement(0);
                                dictPtr[id1] = existingSums.GetElement(1);
                                dictPtr[id2] = existingSums.GetElement(2);
                                dictPtr[id3] = existingSums.GetElement(3);
                                dictPtr[id4] = existingSums.GetElement(4);
                                dictPtr[id5] = existingSums.GetElement(5);
                                dictPtr[id6] = existingSums.GetElement(6);
                                dictPtr[id7] = existingSums.GetElement(7);
                            }
                        }

                        for (; j < list.Count; j++)
                        {
                            var (id, tokenVal) = list[j];
                            dictPtr[id] += tokenVal * val;
                        }
                    }

                    int bestId = -1;
                    float bestDp = float.MinValue;
                    
                    for (int i = 0; i <= MaxId; i++)
                    {
                        var dp = dictPtr[i];
                        if (dp > bestDp)
                        {
                            bestDp = dp;
                            bestId = i;
                        }
                    }
                    if (bestId == -1 || bestDp <= 0)
                    {
                        return (-1, default);
                    }
                    
                    return (bestId, bestDp);
                }
            }
            finally
            {
                ArrayPool<float>.Shared.Return(dict);
            }
        }

        /// <summary>
        /// Retrieve k nearest neighbors from index (can be fewer than k), sorted in descending order of similarity.
        /// Neighbors must have dot product > 0 due to the inner workings of this indexing structure.
        /// </summary>
        /// <param name="vector">The vector whose neighbors should be retrieved</param>
        /// <param name="k">The number of neighbors to retrieve</param>
        /// <returns>Corresponding identifiers of the k (or fewer) nearest neighbors.</returns>
        public List<(int id, float dotProduct)> GetKNearestVectors(FlexibleVector vector, int k)
        {
            if (!vector.IsSparse)
                vector = vector.ToSparse();
            var indexes = vector.Indexes;
            var values = vector.Values;
            if (_intFloatDictionaryBag.TryTake(out var dict))
                dict.Clear();
            else
                dict = new Dictionary<int, float>();
            try
            {
                for (int i = 0; i < indexes.Length; i++)
                {
                    if (!_tokenToVectorsMap.TryGetValue(indexes[i], out var list))
                        continue;
                    var val = values[i];
                    for (int j = 0; j < list.Count; j++)
                    {
                        var (id, tokenVal) = list[j];
                        dict.AddToItem(id, tokenVal * val);
                    }
                }

                var len = Math.Min(dict.Count, k);

                var resList = new List<(int id, float dotProduct)>(len + 1);
                if (len == 0)
                    return resList;

                using (var e = dict.GetEnumerator())
                {
                    for (int i = 0; i < len;)
                    {
                        if (e.MoveNext())
                        {
                            if (e.Current.Value > 0)
                            {
                                resList.Add((e.Current.Key, e.Current.Value));
                                i++;
                            }
                        }
                        else
                            break;
                    }
                    if (resList.Count > 1)
                        resList.Sort(DpResultComparator.Default);
                    if (dict.Count <= len)
                        return resList;
                    while (e.MoveNext())
                    {
                        var curVal = e.Current.Value;
                        if (curVal <= 0)
                            continue;
                        int index = resList.BinarySearch((default, curVal), DpResultComparator.Default);
                        if (index < 0) index = ~index;
                        if (index < resList.Count)
                        {
                            resList.Insert(index, (e.Current.Key, curVal));
                            resList.RemoveAt(len);
                        }
                    }
                }
                return resList;
            }
            finally
            {

                _intFloatDictionaryBag.Add(dict);
            }
        }

        /// <summary>
        /// Get nearby vectors (i.e., those that have dot product > 0) and their dot product with the query vector.
        /// The order of the items in the returned list is arbitrary.
        /// </summary>
        /// <param name="vector"></param>
        /// <returns></returns>
        public List<(int id, float dotProduct)> GetNearbyVectors(FlexibleVector vector)
        {
            if (!vector.IsSparse)
                vector = vector.ToSparse();
            var indexes = vector.Indexes;
            var values = vector.Values;
            if (_intFloatDictionaryBag.TryTake(out var dict))
                dict.Clear();
            else
                dict = new Dictionary<int, float>();
            for (int i = 0; i < indexes.Length; i++)
            {
                if (!_tokenToVectorsMap.TryGetValue(indexes[i], out var list))
                    continue;
                var val = values[i];
                for (int j = 0; j < list.Count; j++)
                {
                    var (id, tokenVal) = list[j];
                    dict.AddToItem(id, tokenVal * val);
                }
            }

            var resList = new List<(int id, float dotProduct)>(dict.Count);
            foreach (var p in dict)
            {
                if (p.Value > 0)
                    resList.Add((p.Key, p.Value));
            }
            _intFloatDictionaryBag.Add(dict);
            return resList;


        }

        private class DpResultComparator : IComparer<(int id, float dotProduct)>
        {
            public static readonly DpResultComparator Default = new DpResultComparator();
            public int Compare((int id, float dotProduct) x, (int id, float dotProduct) y)
            {
                return Comparer<float>.Default.Compare(y.Item2, x.Item2);
            }
        }


        private const string StorageMetaId = "sdpindex-meta.json";
        private const string StorageBlobId = "sdpindex.bin";
        private const string StoragePrefix = "sdpindex-";
        private const int StorageBlobSignature = 4149865;


        private class StorageMeta
        {
            public int Version { get; set; } = 1;
            public int VectorsCount { get; set; }
            public int MaxId { get; set; }
        }

        /// <summary>
        /// Dump indexing structure to the specified file as ZIP archive.
        /// Will update archive if file exists.
        /// </summary>
        /// <param name="fn"></param>
        public void ToFile(string fn)
        {
            var fExists = File.Exists(fn);
            using var stream = new FileStream(fn, FileMode.OpenOrCreate);
            using var zip = new ZipArchive(stream, fExists ? ZipArchiveMode.Update : ZipArchiveMode.Create);
            ToArchive(zip);
        }

        /// <summary>
        /// Dump indexing structure to specified archive.
        /// </summary>
        /// <param name="zip"></param>
        public void ToArchive(ZipArchive zip)
        {
            var meta = new StorageMeta
            {
                MaxId = MaxId,
                VectorsCount = VectorsCount,
                Version = 1
            };

            var metaStr = JsonSerializer.Serialize(meta);
            if (zip.Mode == ZipArchiveMode.Update)
                zip.GetEntry(StorageMetaId)?.Delete();
            var entry = zip.CreateEntry(StorageMetaId);
            using (var entryStream = new StreamWriter(entry.Open()))
                entryStream.Write(metaStr);

            if (zip.Mode == ZipArchiveMode.Update)
                zip.GetEntry(StorageBlobId)?.Delete();
            entry = zip.CreateEntry(StorageBlobId);
            using (var w = new BinaryWriter(new BufferedStream(entry.Open())))
            {
                w.Write(StorageBlobSignature);
                w.Write(_indexedVectors.Count);
                foreach (var v in _indexedVectors)
                {
                    w.Write(v.Key);
                    v.Value.ToWriter(w);
                }
                w.Write(_tokenToVectorsMap.Count);
                foreach (var key in _tokenToVectorsMap.Keys)
                {
                    w.Write(key);
                    var l = _tokenToVectorsMap[key];
                    w.Write(l.Count);
                    for (int i = 0; i < l.Count; i++)
                    {
                        w.Write(l[i].id);
                        w.Write(l[i].tokenVal);
                    }
                }
            }
        }

        /// <summary>
        /// Load indexing structure from specified file dump.
        /// </summary>
        /// <param name="fn"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        public static DotProductIndexedVectors FromFile(string fn)
        {
            using var stream = new FileStream(fn, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            using var zip = new ZipArchive(stream, ZipArchiveMode.Read);
            var entry = zip.GetEntry(StorageMetaId);
            if (entry == null)
                throw new Exception($"not a valid {nameof(DotProductIndexedVectors)} storage file");

            StorageMeta meta;
            using (var reader = new StreamReader(entry.Open()))
                meta = JsonSerializer.Deserialize<StorageMeta>(reader.ReadToEnd()) ??
                       throw new Exception("could not deserialize meta data");

            if (meta.Version < 1)
                throw new Exception($"not a valid {nameof(DotProductIndexedVectors)} storage file");

            var blobEntry = zip.GetEntry(StorageBlobId);
            if (blobEntry == null)
                throw new Exception($"not a valid {nameof(DotProductIndexedVectors)} storage file");

            using (var reader = new BinaryReader(new BufferedStream(blobEntry.Open())))
            {
                if (reader.ReadInt32() != StorageBlobSignature)
                    throw new Exception($"not a valid {nameof(DotProductIndexedVectors)} storage file");

                var len = reader.ReadInt32();
                var indexedVectors = new Dictionary<int, FlexibleVector>(len);
                for (int i = 0; i < len; i++)
                {
                    var k = reader.ReadInt32();
                    var v = FlexibleVector.FromReader(reader);
                    indexedVectors.Add(k, v);
                }

                len = reader.ReadInt32();
                var globalMap = new DictList<int, (int id, float tokenVal)>();
                globalMap.EnsureDictCapacity(len);
                var tList = new List<(int id, float tokenVal)>();
                for (int i = 0; i < len; i++)
                {
                    var k = reader.ReadInt32();
                    var listLen = reader.ReadInt32();
                    tList.Clear();
                    tList.EnsureCapacity(listLen);
                    for (int j = 0; j < listLen; j++)
                    {
                        tList.Add((reader.ReadInt32(), reader.ReadSingle()));
                    }
                    globalMap.AddRange(k, tList);
                }

                return new DotProductIndexedVectors(meta, indexedVectors, globalMap);
            }

        }





    }
}
