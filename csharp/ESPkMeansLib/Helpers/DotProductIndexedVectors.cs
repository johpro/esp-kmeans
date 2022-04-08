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
    /// Indexing structure to quickly retrieve indexed vectors
    /// in descending order of their dot product with the specified query vector.
    /// Please note that this indexing structure was designed for dot products greater than zero, which is typical for
    /// text representations that only have positive or zero vector entries, for instance.
    /// That is, it is not possible to retrieve vectors hat have negative dot products.
    /// </summary>
    public class DotProductIndexedVectors
    {

        public int VectorsCount { get; private set; }
        public int MaxId { get; private set; }

        private readonly DictList<int, (int id, float tokenVal)> _tokenToVectorsMap = new();
        //private readonly Dictionary<int, float> _tokenToMaxValueMap = new();
        private readonly Dictionary<int, FlexibleVector> _indexedVectors = new();
        private readonly ConcurrentBag<Dictionary<int, float>> _intFloatDictionaryBag = new();

        private readonly ConcurrentBag<List<(int token, float val, IList<(int id, float val)> list)>> _skippedListsBag =
            new();
        private int _maxListSizeFirstRun = 0;

        private volatile bool _wasChanged = false;
        private volatile bool _allEntriesAreNonNegative = true;

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
            UpdateTokenMaxValueMap();
            foreach (var vec in indexedVectors.Values)
            {
                var values = vec.Values;
                foreach (var v in values)
                {
                    if (v < -0.00001f)
                    {
                        _allEntriesAreNonNegative = false;
                        break;
                    }
                }
            }
        }


        /// <summary>
        /// Clear the index
        /// </summary>
        public void Clear()
        {
            _indexedVectors.Clear();
            _tokenToVectorsMap.Clear();
            _maxListSizeFirstRun = 0;
            VectorsCount = 0;
            MaxId = 0;
            _wasChanged = false;
            _allEntriesAreNonNegative = true;
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
                InternalAdd(vectors[i], i);
            }
            _wasChanged = true;
            Thread.MemoryBarrier();

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
            InternalAdd(v, id);
            _wasChanged = true;
            Thread.MemoryBarrier();
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private void InternalAdd(FlexibleVector v, int id)
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
                var val = values[i];
                if (val == 0)
                    continue;
                _tokenToVectorsMap.AddToList(indexes[i], (id, val));
                if (val < -0.00001f)
                    _allEntriesAreNonNegative = false;
            }
        }


        /// <summary>
        /// For some tokens, the number of affected indexed vectors is quite high, but
        /// in certain conditions we might not need to consider all these affected vectors,
        /// which would save computing resources.
        /// This method therefore determines a suitable threshold for ignoring longer lists
        /// in the first pass.
        /// </summary>
        private void UpdateTokenMaxValueMap()
        {
            var th1 = Math.Max(10, VectorsCount / 40);
            var th2 = Math.Max(10, VectorsCount / 200);
            var th3 = Math.Max(10, VectorsCount / 500);
            _maxListSizeFirstRun = th1;

            var lists = _tokenToVectorsMap.EntryLists;

            if (th1 != th2 && _tokenToVectorsMap.Count > 1000)
            {
                //choose best threshold so that we only target roughly the 5 percent biggest lists
                var c2 = 0;
                var c3 = 0;

                foreach (var list in lists)
                {
                    var c = list?.Count ?? 0;
                    if (c > th3)
                    {
                        c3++;
                        if (c > th2)
                        {
                            c2++;
                        }
                    }
                }

                var maxAffected = _tokenToVectorsMap.Count / 20;
                if (c3 <= maxAffected)
                {
                    _maxListSizeFirstRun = th3;
                }
                else if (c2 <= maxAffected)
                    _maxListSizeFirstRun = th2;
            }

            //for each big list, we only consider the eight entries with the highest associated value
            //in the first pass and skip the remaining ones (we can use 9th value for computing an
            //upper bound of the possible contribution to the dot product)
            foreach (var list in lists)
            {
                if (list == null || list.Count <= _maxListSizeFirstRun)
                    continue;
                BringTopKValuesToTop(list, 9);
            }

            _wasChanged = false;
            Thread.MemoryBarrier();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void EnsureTokenMaxValueMap()
        {
            Thread.MemoryBarrier();
            if (!_wasChanged)
                return;
            lock (_tokenToVectorsMap)
            {
                if (!_wasChanged)
                    return;
                UpdateTokenMaxValueMap();
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
        /// Get the id and the corresponding dot product of the nearest indexed vector of the specified query vector.
        /// Returns id of -1 if no match has been found (with dot product > 0).
        /// Neighbor must have dot product > 0 due to the inner workings of this indexing structure.
        /// </summary>
        /// <param name="vector">Query vector</param>
        /// <param name="minDotProduct">Specify minimum dot product threshold if applicable (could improve performance).
        /// Result could still have lower dot product, though.</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public (int id, float dotProduct) GetNearestVector(FlexibleVector vector, float minDotProduct = float.Epsilon)
        {
            return
                vector.Indexes.Length == 1
                    ? GetNearestVector(vector.Indexes[0], vector.Values[0])
                    : GetNearestVectorInternal(vector, minDotProduct);
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        internal (int id, float dotProduct) GetNearestVector(int index, float tokenVal)
        {
            if (!_tokenToVectorsMap.TryGetValue(index, out var list) || list.Count == 0)
                return (-1, default);
            var bestId = list[0].id;
            var bestVal = list[0].tokenVal;
            if (tokenVal >= 0)
            {
                if (list.Count > _maxListSizeFirstRun)
                {
                    //we know that in this case the highest value and therefore our match is the very first entry
                    return (list[0].id, list[0].tokenVal * tokenVal);
                }
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
        internal (int id, float dotProduct) GetNearestVectorInternal(FlexibleVector vector, float minDotProduct)
        {
            EnsureTokenMaxValueMap();
            if (!vector.IsSparse)
                vector = vector.ToSparse();
            if (_intFloatDictionaryBag.TryTake(out var dict))
                dict.Clear();
            else
                dict = new Dictionary<int, float>();
            try
            {
                var dpIncrementOffset = 0f;
                if (_skippedListsBag.TryTake(out var skippedLists))
                    skippedLists.Clear();
                else
                    skippedLists = new List<(int token, float val, IList<(int id, float val)> list)>();
                try
                {
                    var isHardTh = minDotProduct > 0.04f;
                    var maxDpIncrement = 0.3f;
                    if (isHardTh)
                    {
                        if (minDotProduct >= 0.3f)
                        {
                            maxDpIncrement = minDotProduct - 0.1f;
                        }
                        else
                        {
                            maxDpIncrement = minDotProduct - 0.001f;
                        }
                    }
                    var len = GetNearestVectorsCandidates(1, vector, skippedLists, dict,
                        ref dpIncrementOffset, maxDpIncrement, isHardTh);
                    if (len == 0)
                        return (-1, default);


                    float dpThreshold = minDotProduct - dpIncrementOffset - 0.001f;
                    float topKDotProduct = float.MinValue;
                    if (dict.Count > len)
                    {
                        topKDotProduct = dict.Values.Max();
                        //we only need to look at vectors in dict that have dot product geq the following threshold
                        //since any other vector cannot make the top-k cut in the end
                        dpThreshold = Math.Max(dpThreshold, topKDotProduct - dpIncrementOffset - 0.001f);
                    }

                    var bestId = -1;
                    var bestDp = float.MinValue;


                    foreach (var p in dict)
                    {
                        if (p.Value < dpThreshold)
                            continue;

                        var dp = p.Value;
                        var id = p.Key;
                        if (skippedLists.Count != 0)
                        {
                            //we need to complete the dot product calculations
                            var vec = _indexedVectors[id];
                            foreach ((int token, float val, var list) in skippedLists)
                            {
                                if (!vec.TryGetValue(token, out var vecVal))
                                    continue;

                                //first 8 values have already been considered
                                if (list[7].id == id ||
                                    list[0].id == id ||
                                    list[1].id == id ||
                                    list[2].id == id ||
                                    list[3].id == id ||
                                    list[4].id == id ||
                                    list[5].id == id ||
                                    list[6].id == id)
                                    continue;
                                dp += val * vecVal;
                            }
                        }

                        if (dp > bestDp)
                        {
                            bestDp = dp;
                            bestId = id;
                            if (bestDp > topKDotProduct)
                            {
                                dpThreshold = Math.Max(dpThreshold, bestDp - dpIncrementOffset - 0.001f);
                            }
                        }
                    }

                    if (bestId == -1 || bestDp <= 0)
                        return (-1, default);
                    return (bestId, bestDp);
                }
                finally
                {
                    skippedLists.Clear();
                    _skippedListsBag.Add(skippedLists);
                }


            }
            finally
            {

                _intFloatDictionaryBag.Add(dict);
            }
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
                                var (id7, tokenVal7) = list[j + 7];
                                var (id0, tokenVal0) = list[j];
                                var (id1, tokenVal1) = list[j + 1];
                                var (id2, tokenVal2) = list[j + 2];
                                var (id3, tokenVal3) = list[j + 3];
                                var (id4, tokenVal4) = list[j + 4];
                                var (id5, tokenVal5) = list[j + 5];
                                var (id6, tokenVal6) = list[j + 6];

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
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public List<(int id, float dotProduct)> GetKNearestVectors(FlexibleVector vector, int k)
        {
            EnsureTokenMaxValueMap();
            if (!vector.IsSparse)
                vector = vector.ToSparse();
            if (_intFloatDictionaryBag.TryTake(out var dict))
                dict.Clear();
            else
                dict = new Dictionary<int, float>();
            try
            {
                var dpIncrementOffset = 0f;
                if (_skippedListsBag.TryTake(out var skippedLists))
                    skippedLists.Clear();
                else
                    skippedLists = new List<(int token, float val, IList<(int id, float val)> list)>();
                try
                {

                    var resList = new List<(int id, float dotProduct)>(k + 1);
                    var len = GetNearestVectorsCandidates(k, vector, skippedLists, dict,
                        ref dpIncrementOffset, 0.3f);

                    if (len == 0)
                        return resList;

                    if (dict.Count > k)
                    {
                        var topKDotProduct = GetTopKValue(dict.Values, k, out _);
                        //we only need to look at vectors in dict that have dot product geq the following threshold
                        //since any other vector cannot make the top-k cut in the end
                        var dpThreshold = topKDotProduct - dpIncrementOffset - 0.001f;
                        foreach (var p in dict)
                        {
                            if (p.Value >= dpThreshold)
                                resList.Add((p.Key, p.Value));
                        }

                        if (skippedLists.Count > 0 && skippedLists.Sum(t => t.list.Count)
                            < resList.Count * skippedLists.Count * 3)
                        {
                            //threshold did not lead to significant narrowing of results, cheaper to 
                            //directly add skipped lists
                            foreach ((_, float tokenVal, IList<(int id, float val)> list) in skippedLists)
                            {
                                AddTokenValListToDictionary(dict, tokenVal, list, 8);
                            }

                            skippedLists.Clear();
                            dpIncrementOffset = 0;
                        }
                    }
                    else
                    {
                        foreach (var p in dict)
                        {
                            resList.Add((p.Key, p.Value));
                        }
                    }

                    if (skippedLists.Count != 0)
                    {
                        CompleteDotProductsOfSkippedTokens(resList, skippedLists);
                    }

                    resList.Sort(DpResultComparator.Default);
                    var toRemoveIdx = 0;
                    for (; toRemoveIdx < resList.Count && toRemoveIdx < k; toRemoveIdx++)
                    {
                        if (resList[toRemoveIdx].dotProduct <= 0)
                            break;
                    }
                    if (toRemoveIdx < resList.Count)
                        resList.RemoveRange(toRemoveIdx, resList.Count - toRemoveIdx);

                    return resList;
                }
                finally
                {
                    skippedLists.Clear();
                    _skippedListsBag.Add(skippedLists);
                }

            }
            finally
            {

                _intFloatDictionaryBag.Add(dict);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private void CompleteDotProductsOfSkippedTokens(List<(int id, float dotProduct)> resList, List<(int token, float val, IList<(int id, float val)> list)> skippedLists)
        {
            //we need to complete the dot product calculations for the final candidates
            for (int i = 0; i < resList.Count; i++)
            {
                (int id, float dotProduct) = resList[i];
                var vec = _indexedVectors[id];
                foreach ((int token, float val, var list) in skippedLists)
                {
                    if (!vec.TryGetValue(token, out var vecVal))
                        continue;

                    //first 8 values have already been considered
                    if (list[7].id == id ||
                        list[0].id == id ||
                        list[1].id == id ||
                        list[2].id == id ||
                        list[3].id == id ||
                        list[4].id == id ||
                        list[5].id == id ||
                        list[6].id == id)
                        continue;
                    dotProduct += val * vecVal;
                }

                resList[i] = (id, dotProduct);
            }

            skippedLists.Clear();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private static void AddTokenValListToDictionary(Dictionary<int, float> dict, float val,
            IList<(int id, float tokenVal)> list, int offset = 0, int count = -1)
        {
            const int vecSize = 8;
            if (Avx.IsSupported && vecSize != Vector256<float>.Count)
                throw new Exception("AVX vector size mismatch");

            if (count < 0)
                count = list.Count - offset;
            var j = offset;
            var endLimit = offset + count;
            if (Avx.IsSupported && count >= vecSize)
            {
                var limit = endLimit - vecSize;
                var valVec = Vector256.Create(val);
                for (; j <= limit; j += vecSize)
                {
                    var (id7, tokenVal7) = list[j + 7];
                    var (id0, tokenVal0) = list[j];
                    var (id1, tokenVal1) = list[j + 1];
                    var (id2, tokenVal2) = list[j + 2];
                    var (id3, tokenVal3) = list[j + 3];
                    var (id4, tokenVal4) = list[j + 4];
                    var (id5, tokenVal5) = list[j + 5];
                    var (id6, tokenVal6) = list[j + 6];

                    var tokenValVec = Vector256.Create(tokenVal0, tokenVal1, tokenVal2, tokenVal3,
                        tokenVal4, tokenVal5, tokenVal6, tokenVal7);

                    ref var dictRef0 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id0, out _);
                    ref var dictRef1 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id1, out _);
                    ref var dictRef2 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id2, out _);
                    ref var dictRef3 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id3, out _);
                    ref var dictRef4 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id4, out _);
                    ref var dictRef5 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id5, out _);
                    ref var dictRef6 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id6, out _);
                    ref var dictRef7 = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, id7, out _);

                    var existingSums = Vector256.Create(dictRef0, dictRef1, dictRef2, dictRef3, dictRef4,
                        dictRef5, dictRef6, dictRef7);

                    if (Fma.IsSupported)
                    {
                        existingSums = Fma.MultiplyAdd(valVec, tokenValVec, existingSums);
                    }
                    else
                    {
                        var mul = Avx.Multiply(valVec, tokenValVec);
                        existingSums = Avx.Add(existingSums, mul);
                    }

                    dictRef0 = existingSums.GetElement(0);
                    dictRef1 = existingSums.GetElement(1);
                    dictRef2 = existingSums.GetElement(2);
                    dictRef3 = existingSums.GetElement(3);
                    dictRef4 = existingSums.GetElement(4);
                    dictRef5 = existingSums.GetElement(5);
                    dictRef6 = existingSums.GetElement(6);
                    dictRef7 = existingSums.GetElement(7);
                }
            }

            for (; j < endLimit; j++)
            {
                var (id, tokenVal) = list[j];
                dict.AddToItem(id, tokenVal * val);
            }
        }


        /// <summary>
        /// Go through non-zero entries of query vector and retrieve all indexed vectors that have at least one
        /// overlapping non-zero entry. It applies some shortcuts to narrow the set of returned vectors given that we may
        /// only want to retrieve the top k indexed vectors or vectors above a certain specified min dot product.
        /// The dictionary (dict) contains all retrieved vectors ids and their corresponding dot product, but this
        /// dot product could be incomplete if one or more larger lists have been omitted as part of the shortcuts
        /// (will be stored in skippedLists).
        /// </summary>
        /// <param name="k">Number of nearest vectors that we want to retrieve at least</param>
        /// <param name="vector">Query vector</param>
        /// <param name="skippedLists">Skipped larger token->vector lists (only first 8 vector ids have been applied)</param>
        /// <param name="dict">Affected indexed vector ids and their (partially) computed dot product with the query vector</param>
        /// <param name="dpIncrementOffset">The actual dot product could be higher up to this value</param>
        /// <param name="maxDpIncrementOffset">Maximum deviation from the actual dot product we want to allow</param>
        /// <param name="isHardThreshold">if true, maximum deviation that is specified is also min dot product threshold for results</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private int GetNearestVectorsCandidates(int k, FlexibleVector vector, List<(int token, float val, IList<(int id, float val)> list)> skippedLists,
        Dictionary<int, float> dict, ref float dpIncrementOffset, float maxDpIncrementOffset, bool isHardThreshold = false)
        {

            var indexes = vector.Indexes;
            var values = vector.Values;
            var tokenLists = ArrayPool<(int token, float val, IList<(int id, float val)> list)>
                .Shared.Rent(indexes.Length);
            var numLists = 0;
            try
            {
                if (!_allEntriesAreNonNegative)
                    maxDpIncrementOffset = float.MinValue; //shortcut does not work with negative values

                //first retrieve all lists so that we can sort them in descending order of their size
                for (int i = 0; i < indexes.Length; i++)
                {
                    var idx = indexes[i];
                    if (!_tokenToVectorsMap.TryGetValue(idx, out var list))
                        continue;

                    var val = values[i];
                    tokenLists[numLists] = (idx, val, list);
                    numLists++;
                }

                if (numLists == 0)
                    return 0;
                //sort lists in descending order of their size so that we have high chance to skip the longest ones
                Array.Sort(tokenLists, 0, numLists, TokenListComparator.Default);

                for (int i = 0; i < numLists; i++)
                {
                    var (idx, val, list) = tokenLists[i];
                    if (list.Count > _maxListSizeFirstRun)
                    {
                        //skip most parts of long lists for now.
                        //first 9 items are highest values and sorted in descending order.
                        //we apply the first 8 items and compute the upper bound of the product for
                        //the remaining ones based on the 9th item (given that this upper bound does not
                        //lead to higher total dot product deviation than allowed)

                        var maxTokenVal = list[8].val;
                        var maxDpIncrement = val * maxTokenVal;
                        var newOffset = dpIncrementOffset + maxDpIncrement;

                        if (newOffset < maxDpIncrementOffset)
                        {
                            skippedLists.Add((idx, val, list));
                            dpIncrementOffset = newOffset;

                            AddTokenValListToDictionary(dict, val, list, 0, 8);
                            continue;
                        }
                    }

                    AddTokenValListToDictionary(dict, val, list);
                }
            }
            finally
            {
                ArrayPool<(int token, float val, IList<(int id, float val)> list)>
                    .Shared.Return(tokenLists, true);
            }


            var len = Math.Min(dict.Count, k);
            if (isHardThreshold)
                return len;
            if (skippedLists.Count == 0) return len;
            //we have to make sure that the resulting dict contains at least the actual top k nearest neighbors
            bool goThroughSkippedLists;
            if (len < k)
                goThroughSkippedLists = true;
            else
            {
                var itemsAboveIncrement = 0;
                foreach (var val in dict.Values)
                {
                    if (val > dpIncrementOffset)
                    {
                        itemsAboveIncrement++;
                        if (itemsAboveIncrement >= k)
                            break; //we know that indexed vectors not in dict cannot lead to dp > increment
                    }
                }
                goThroughSkippedLists = itemsAboveIncrement < k;
            }

            if (goThroughSkippedLists)
            {
                foreach ((_, float tokenVal, IList<(int id, float val)> list) in skippedLists)
                {
                    AddTokenValListToDictionary(dict, tokenVal, list, 8);
                }

                skippedLists.Clear();
                dpIncrementOffset = 0;
            }


            return len;
        }

        /// <summary>
        /// Find the highest k values in the specified list and move the corresponding entries to the top of the list
        /// in descending order of their value.
        /// </summary>
        /// <param name="list"></param>
        /// <param name="k"></param>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static void BringTopKValuesToTop(List<(int id, float val)> list, int k)
        {
            if (list.Count <= k)
            {
                list.Sort(DpResultComparator.Default);
                return;
            }
            list.Sort(0, k, DpResultComparator.Default);
            for (int i = k; i < list.Count; i++)
            {
                var curVal = list[i];
                int index = list.BinarySearch(0, k, curVal, DpResultComparator.Default);
                if (index < 0) index = ~index;
                if (index < k)
                {
                    //insert curVal at index and move others one down the list = move element at pos k to i so that
                    //we only need to move some values at the top
                    list[i] = list[k];
                    for (int j = k - 1; j >= index; j--)
                    {
                        list[j + 1] = list[j];
                    }

                    list[index] = curVal;
                }
            }
        }

        /// <summary>
        /// Find the top-k value in the specified list (value at position k-1 if list was sorted in descending order)
        /// </summary>
        /// <param name="values"></param>
        /// <param name="k"></param>
        /// <param name="actualK"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static float GetTopKValue(IEnumerable<float> values, int k, out int actualK)
        {
            var arr = ArrayPool<float>.Shared.Rent(k + 1);
            try
            {
                using (var e = values.GetEnumerator())
                {
                    int i = 0;
                    for (; i < k;)
                    {
                        if (e.MoveNext())
                        {
                            arr[i] = e.Current;
                            i++;
                        }
                        else
                            break;
                    }

                    if (i == 0)
                    {
                        actualK = 0;
                        return default;
                    }

                    actualK = i;
                    if (i < k)
                    {
                        var res = arr[0];
                        for (int j = 1; j < i; j++)
                        {
                            res = Math.Min(res, arr[j]);
                        }

                        return res;
                    }

                    Array.Sort(arr, 0, i);
                    while (e.MoveNext())
                    {
                        var curVal = e.Current;
                        int index = Array.BinarySearch(arr, 0, i, curVal);
                        if (index < 0) index = ~index;
                        if (index > 0)
                        {
                            //we want to cut list at the beginning
                            index--;

                            if (index > 0)
                            {
                                Array.Copy(arr, 1, arr, 0, index);
                            }

                            arr[index] = curVal;
                        }
                    }

                    return arr[0];
                }
            }
            finally
            {
                ArrayPool<float>.Shared.Return(arr);
            }
        }

        /// <summary>
        /// Get indexed nearby vectors (i.e., those that have dot product > 0) and their dot product with the query vector.
        /// The order of the items in the returned list is arbitrary.
        /// </summary>
        /// <param name="vector"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public List<(int id, float dotProduct)> GetNearbyVectors(FlexibleVector vector)
        {
            return vector.Indexes.Length == 1
                ? GetNearbyVectors(vector.Indexes[0], vector.Values[0])
                : GetNearbyVectorsFull(vector);
        }

        /// <summary>
        /// Get all nearby indexed vectors of the query vector that have a dot product equal to or above the specified threshold (which has to be greater 0).
        /// The order of the items in the returned list is arbitrary, and the actual dot product may also be lower (it is just
        /// guaranteed that no indexed vector is missing that meets the dot product threshold).
        /// </summary>
        /// <param name="vector"></param>
        /// <param name="dotProductThreshold"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public List<(int id, float dotProduct)> GetNearbyVectors(FlexibleVector vector, float dotProductThreshold)
        {
            if (!_allEntriesAreNonNegative)
                return GetNearbyVectors(vector);

            if (dotProductThreshold <= 0)
                throw new ArgumentException("dot product threshold must be greater than zero");
            return vector.Indexes.Length == 1
                ? GetNearbyVectors(vector.Indexes[0], vector.Values[0], dotProductThreshold)
                : GetNearbyVectorsThreshold(vector, dotProductThreshold);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private List<(int id, float dotProduct)> GetNearbyVectorsFull(FlexibleVector vector)
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
                AddTokenValListToDictionary(dict, val, list);
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

        private List<(int id, float dotProduct)> GetNearbyVectors(int tokenIndex, float tokenValue, float minDotProduct = 0f)
        {
            if (!_tokenToVectorsMap.TryGetValue(tokenIndex, out var list) || list.Count == 0)
                return new List<(int id, float dotProduct)>(0);
            var resList = new List<(int id, float dotProduct)>(list.Count);
            for (int j = 0; j < list.Count; j++)
            {
                var (id, tokenVal) = list[j];
                var dp = tokenVal * tokenValue;
                if (dp > 0 && dp >= minDotProduct)
                    resList.Add((id, dp));
            }
            return resList;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private List<(int id, float dotProduct)> GetNearbyVectorsThreshold(FlexibleVector vector, float dpThreshold)
        {
            EnsureTokenMaxValueMap();
            if (!vector.IsSparse)
                vector = vector.ToSparse();
            if (_intFloatDictionaryBag.TryTake(out var dict))
                dict.Clear();
            else
                dict = new Dictionary<int, float>();
            try
            {
                var dpIncrementOffset = 0f;
                if (_skippedListsBag.TryTake(out var skippedLists))
                    skippedLists.Clear();
                else
                    skippedLists = new List<(int token, float val, IList<(int id, float val)> list)>();
                try
                {
                    GetNearestVectorsCandidates(int.MaxValue, vector, skippedLists, dict,
                        ref dpIncrementOffset, dpThreshold >= 0.2f
                            ? dpThreshold - 0.1f
                            : (dpThreshold >= 0.1f
                                ? dpThreshold - 0.05f
                                : dpThreshold - 0.001f), true);

                    var resList = new List<(int id, float dotProduct)>(dict.Count);
                    if (dict.Count == 0)
                        return resList;
                    var threshold = dpThreshold - dpIncrementOffset - 0.001f;

                    if (skippedLists.Count > 10)
                    {
                        var numAboveTh = 0;
                        var maxNumAboveTh = _indexedVectors.Count / 4;
                        foreach (var dp in dict.Values)
                        {
                            if (dp >= threshold)
                            {
                                numAboveTh++;
                                if (numAboveTh > maxNumAboveTh)
                                    break;
                            }
                        }

                        if (numAboveTh > maxNumAboveTh)
                        {
                            //with that many results its faster to just apply all skipped lists than completing dot products
                            foreach ((_, float tokenVal, IList<(int id, float val)> list) in skippedLists)
                            {
                                AddTokenValListToDictionary(dict, tokenVal, list, 8);
                            }

                            skippedLists.Clear();
                            threshold = dpThreshold;
                        }
                    }

                    foreach (var p in dict)
                    {
                        if (p.Value >= threshold)
                            resList.Add((p.Key, p.Value));
                    }

                    if (skippedLists.Count != 0)
                    {
                        CompleteDotProductsOfSkippedTokens(resList, skippedLists);
                    }
                    return resList;
                }
                finally
                {
                    skippedLists.Clear();
                    _skippedListsBag.Add(skippedLists);
                }

            }
            finally
            {

                _intFloatDictionaryBag.Add(dict);
            }
        }

        private class DpResultComparator : IComparer<(int id, float dotProduct)>
        {
            public static readonly DpResultComparator Default = new DpResultComparator();
            public int Compare((int id, float dotProduct) x, (int id, float dotProduct) y)
            {
                return Comparer<float>.Default.Compare(y.Item2, x.Item2);
            }
        }

        private class TokenListComparator : IComparer<(int token, float val, IList<(int id, float val)> list)>
        {
            public static readonly TokenListComparator Default = new TokenListComparator();
            public int Compare((int token, float val, IList<(int id, float val)> list) x, (int token, float val, IList<(int id, float val)> list) y)
            {
                return Comparer<float>.Default.Compare(y.list.Count, x.list.Count);
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
