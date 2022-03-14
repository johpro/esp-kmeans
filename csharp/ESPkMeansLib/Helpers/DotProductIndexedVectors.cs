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
    /// Indexing structure for unit-length vectors.
    /// Given a minimum dot product of Lambda, retrieve all indexed unit-length vectors
    /// that can have a dot product of at least Lambda with the query vector (of length smaller or equal 1).
    /// The respective dot product of the returned vectors with the query may still be lower than the provided threshold,
    /// but it is guaranteed that none of the remaining vectors in the index would match the query.
    /// Several such Lambdas can be defined in the constructor to build a layered index.
    /// Given a query vector and a lower bound of the dot product, the structure will then use the layer for retrieval
    /// that has the highest threshold which is still equal to or below the lower bound, which makes retrieval operations
    /// more efficient.
    /// Please note that this indexing structure was designed for dot products in the range (0,1], which is typical for
    /// text representations that only have positive or zero vector entries, for instance.
    /// That is, it is not possible to retrieve vectors based on a negative threshold (for instance, >= -0.1).
    /// A threshold of zero thus implies an actual threshold of zero+epsilon.
    /// </summary>
    public class DotProductIndexedVectors
    {

        public float MinDotProduct { get; }
        public int VectorsCount { get; private set; }
        public int MaxId { get; private set; }

        public static readonly float[] DefaultThresholds = { 0.1f, 0.25f, 0.4f, 0.6f };

        class DotProductThItem
        {
            /// <summary>
            /// given a token, get list of affected vectors. For each vector id, we also store
            /// how many tokens in source query need to share non-zero entry with vector such that
            /// we get at least required dot product.
            /// </summary>
            public readonly DictList<int, (int id, int minNumOccurrences)> TokenToVectorsMap = new();


            public float MinDotProduct;
            public float MinDotProductSquared;
            public float MaxSquaredSum;

            public void Clear()
            {
                TokenToVectorsMap.Clear();
            }

            public void ToWriter(BinaryWriter writer)
            {
                writer.Write(1); //version
                writer.Write(MinDotProduct);
                writer.Write(MinDotProductSquared);
                writer.Write(MaxSquaredSum);
                writer.Write(TokenToVectorsMap.Count);
                foreach (var key in TokenToVectorsMap.Keys)
                {
                    var l = TokenToVectorsMap[key];
                    writer.Write(key);
                    writer.Write(l.Count);
                    for (int i = 0; i < l.Count; i++)
                    {
                        var (id, num) = l[i];
                        writer.Write(id);
                        writer.Write(num);
                    }
                }
            }

            public static DotProductThItem FromReader(BinaryReader reader)
            {
                var version = reader.ReadInt32();
                var dp = reader.ReadSingle();
                var dpSquared = reader.ReadSingle();
                var maxSqSum = reader.ReadSingle();
                
                var res = new DotProductThItem
                {
                    MinDotProduct = dp,
                    MinDotProductSquared = dpSquared,
                    MaxSquaredSum = maxSqSum
                };

                var dictCount = reader.ReadInt32();
                res.TokenToVectorsMap.EnsureDictCapacity(dictCount);
                var list = new List<(int id, int minNumOccurrences)>();
                for (int i = 0; i < dictCount; i++)
                {
                    var key = reader.ReadInt32();
                    var listCount = reader.ReadInt32();
                    list.Clear();
                    list.EnsureCapacity(listCount);
                    for (int j = 0; j < listCount; j++)
                    {
                        list.Add((reader.ReadInt32(), reader.ReadInt32()));
                    }
                    res.TokenToVectorsMap.AddRange(key, list);
                }

                return res;
            }


        }

        private readonly bool _hasOnlyZeroThreshold;
        private readonly DotProductThItem[] _map;
        private readonly DictListInt<int> _globalMap = new();
        private readonly Dictionary<int, FlexibleVector> _indexedVectors = new();

        private readonly ConcurrentBag<HashSet<int>> _vectorHashSetBag = new();
        private readonly ConcurrentBag<Dictionary<int, int>> _vectorDictionaryBag = new();
        private readonly ConcurrentBag<List<int>> _intListBag = new();
        private static readonly Dictionary<int, int> EmptyDict = new(0);



        /// <summary>
        /// Create indexing structure based on default thresholds (0.1, 0.25, 0.4f, and 0.6).
        /// </summary>
        public DotProductIndexedVectors() : this(DefaultThresholds)
        {
        }

        /// <summary>
        /// Create indexing structure.
        /// </summary>
        /// <param name="minDotProductValues">One or several dot product thresholds (>= 0) that should be used for indexing the vectors</param>
        public DotProductIndexedVectors(float[] minDotProductValues)
        {
            /*
             *  Lemma 1: given a vector v with at most unit-length, then
                    the vector v/|v| maximizes the dot product with v among all
                    possible unit-length vectors

                    Proof: v dot x = |v| |x| cos(theta) -> |x| is 1, hence all vectors that have
                        angle 0 (cos(0) = 1) maximize dot product
                        -> all scaled versions of v, can only be v/|v| since 1/|v| is only scalar
                        that leads to unit-length vector

                Lemma 2: any vector v can only then (if at all) lead to a dot product >= minDotProduct
                    with a query vector of unit length x,
                    if its length is >= minDotProduct (=> squared sum >= minDotProduct ^2)
                    
                    Proof: v dot x = |v| |x| cos(theta) -> |x| is 1, cos(theta) <= 1 -> v dot x <= |v|

                from Lemma 2 follows that if a subset of entries of a unit-length vector v
                    already has squared sum > 1- minDotProduct^2,
                    then a query vector containing only the remaining entries of v (or a subset)
                    can never lead to a dot product >= minDotProduct

                Lemma 3: given value > 0 && < minDotProduct, then we would need at least minDotProduct^2/value^2
                        of tokens with <= value so that we can reach minDotProduct
             */
            var minDotProductValues1 = minDotProductValues.ToArray();
            if (minDotProductValues1.Length == 1 && minDotProductValues1[0] <= float.Epsilon)
            {
                _hasOnlyZeroThreshold = true;
            }

            Array.Sort(minDotProductValues1);
            MinDotProduct = minDotProductValues1[0];

            _map = new DotProductThItem[minDotProductValues1.Length];
            for (int i = 0; i < _map.Length; i++)
            {
                var minDotProduct = minDotProductValues1[i];
                if (minDotProduct < 0)
                    throw new ArgumentException("indexing structure does not support negative dot product threshold");
                _map[i] = new DotProductThItem
                {
                    MinDotProduct = minDotProduct,
                    MinDotProductSquared = minDotProduct * minDotProduct,
                    MaxSquaredSum = 1 - minDotProduct * minDotProduct
                };
            }
        }

        private DotProductIndexedVectors(StorageMeta meta, DotProductThItem[] maps,
            Dictionary<int, FlexibleVector> indexedVectors, DictListInt<int> globalMap)
        {
            MinDotProduct = meta.MinDotProduct;
            MaxId = meta.MaxId;
            VectorsCount = meta.VectorsCount;
            _map = maps;
            _indexedVectors = indexedVectors;
            _globalMap = globalMap;
            if (maps.Length == 1 && maps[0].MinDotProduct <= float.Epsilon)
            {
                _hasOnlyZeroThreshold = true;
            }
        }
        

        /// <summary>
        /// Clear the index
        /// </summary>
        public void Clear()
        {
            _indexedVectors.Clear();
            foreach (var dict in _map)
            {
                dict.Clear();
            }
            _globalMap.Clear();
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
            if(_globalMap.AvailableCount == 0)
                PrepareGlobalMapCapacity(vectors);
            Clear();
            _indexedVectors.EnsureCapacity(vectors.Length);
            for (var i = 0; i < vectors.Length; i++)
            {
                Add(vectors[i], i);
            }
        }

        /// <summary>
        /// Pass through all vectors to count distribution of indexes so that we can initialize the capacity of the global map
        /// </summary>
        /// <param name="vectors"></param>
        private void PrepareGlobalMapCapacity(FlexibleVector[] vectors)
        {
            var countDict = new Dictionary<int, int>(vectors.Max(v => v.Length));
            foreach (var v in vectors)
            {
                var indexes = v.Indexes;
                for (int i = 0; i < indexes.Length; i++)
                {
                    countDict.IncrementItem(indexes[i]);
                }
            }
            _globalMap.EnsureDictCapacity(countDict.Count);
            foreach (var p in countDict)
            {
                if(p.Value <= 1)
                    continue;
                _globalMap.EnsureListCapacity(p.Key, p.Value);
            }
        }

        /// <summary>
        /// Add provided unit-length vector to the index. A unique identifier has to be provided as well, which will be returned on querying the index.
        /// </summary>
        /// <param name="v">Vector to add</param>
        /// <param name="id">Unique number to identify vector</param>
        /// <exception cref="ArgumentException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public unsafe void Add(FlexibleVector v, int id)
        {
            if (v.Length < 1)
                throw new ArgumentException("cannot add zero vector to index");
            if (!v.IsSparse)
                v = v.ToSparse();
            if (!v.IsUnitVector)
                throw new ArgumentException("vector has to have length 1");
            VectorsCount++;
            MaxId = Math.Max(MaxId, id);
            _indexedVectors.Add(id, v);

            //in every case we need to populate global map

            if (_hasOnlyZeroThreshold)
            {
                //we just need global map
                AddToGlobalMap(v.Indexes, id);
                return;
            }


            //sorting and squaring makes up around 20% of the indexing time
            var numZero = 0;
            var len = SortValues(v, out var indexes, out var values);
            var valuesSquared = ArrayPool<float>.Shared.Rent(len);
            fixed (float* valuesSquaredPtr = valuesSquared, valuesPtr = values)
            {
                Square(len, valuesPtr, valuesSquaredPtr);

                for (; numZero < len; numZero++)
                {
                    if (valuesPtr[numZero] > float.MinValue)
                        break;
                }
            }

            len -= numZero;
            var indexesSpan = indexes.AsSpan(numZero, len);
            AddToGlobalMap(indexesSpan, id);
            AddWithCountStrategy(indexesSpan,
                values.AsSpan(numZero, len), valuesSquared.AsSpan(numZero, len), id);
            ArrayPool<int>.Shared.Return(indexes);
            ArrayPool<float>.Shared.Return(values);
            ArrayPool<float>.Shared.Return(valuesSquared);
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private void AddToGlobalMap(ReadOnlySpan<int> indexes, int id)
        {
            for (var i = 0; i < indexes.Length; i++)
            {
                var idx = indexes[i];
                _globalMap.AddToList(idx, id);
            }
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private static int SortValues(FlexibleVector vec, out int[] indexes, out float[] values)
        {
            var len = vec.Length;
            indexes = ArrayPool<int>.Shared.Rent(len);
            //vec.Indexes.CopyTo(indexes);
            values = ArrayPool<float>.Shared.Rent(len);
            //vec.Values.CopyTo(values);
            vec.CopyTo(indexes, values);
            //we are only interested in absolute values
            for (int i = 0; i < len; i++)
            {
                var val = values[i];
                if (val < 0)
                    values[i] = Math.Abs(val);
            }
            Array.Sort(values, indexes, 0, len);
            return len;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private unsafe void AddWithCountStrategy(Span<int> indexes, Span<float> values, Span<float> valuesSquared, int id)
        {
            //pointer-based access leads to slight gains in performance of a about 10-15%
            //map.TokenToVectorsMap.AddToList takes most of the time
            fixed (int* indexesPtr = indexes)
            {
                fixed (float* valuesPtr = values, valuesSquaredPtr = valuesSquared)
                {
                    foreach (var map in _map)
                    {
                        var currentEndIdxOfSumSpan = values.Length - 1;
                        var squaredSumOfSumSpan = 0f;

                        var minDotProduct = map.MinDotProduct;
                        var requiredSquaredSum = map.MinDotProductSquared - 0.001f;

                        var minNumTokens = 1;

                        for (int i = values.Length - 1; i >= 0; i--)
                        {
                            var val = valuesPtr[i];
                            var idx = indexesPtr[i];
                            if (val >= minDotProduct)
                            {
                                //if only this token is present in query it could already be enough to get
                                //required dot product
                                map.TokenToVectorsMap.AddToList(idx, (id, 1));
                                currentEndIdxOfSumSpan = i - 1;
                                continue;
                            }

                            //now determine min num of tokens with a non-zero entry in this vector
                            //to retrieve required min dot product, given that none of the previous (higher-valued)
                            //tokens were present -> minNumTokens increases monotonically

                            //-> we can do this similar to a "sliding window" approach so that we avoid squared time complexity
                            if (squaredSumOfSumSpan > 0)
                            {
                                squaredSumOfSumSpan -= valuesSquaredPtr[i + 1];
                            }

                            var isDone = false;
                            squaredSumOfSumSpan += valuesSquaredPtr[currentEndIdxOfSumSpan];
                            currentEndIdxOfSumSpan--;
                            while (squaredSumOfSumSpan < requiredSquaredSum)
                            {
                                if (currentEndIdxOfSumSpan < 0)
                                {
                                    //we cannot achieve required sum anymore with this or next tokens
                                    isDone = true;
                                    break;
                                }
                                squaredSumOfSumSpan += valuesSquaredPtr[currentEndIdxOfSumSpan];
                                currentEndIdxOfSumSpan--;
                                minNumTokens++;
                            }

                            if (isDone)
                                break;

                            map.TokenToVectorsMap.AddToList(idx, (id, minNumTokens));
                        }
                    }
                }
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
        /// Returns id of -1 if no match has been found.
        /// Neighbors must have dot product > 0 due to the inner workings of this indexing structure.
        /// A threshold > 0 (<paramref name="minDotProduct"/>) can be defined
        /// to ignore neighbors that are too far away.
        /// </summary>
        /// <param name="vector"></param>
        /// <param name="minDotProduct"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public (int id, float dotProduct) GetNearestVector(FlexibleVector vector, float minDotProduct = 0)
        {
            if (minDotProduct < 0)
                throw new ArgumentException("indexing structure does not support negative dot product threshold");
            if(vector.Length == 0)
                return (-1, default);
            var countDict = EmptyDict;
            try
            {
                //count occurrences with global map first so that we do not need to repeat this for every threshold
                int offset = 0;
                if (!_hasOnlyZeroThreshold)
                    countDict = CountAffectedClusters(vector.Indexes, out offset);
                var lowerLimit = _map[0].MinDotProduct <= float.Epsilon ? 0 : -1;
                for (int i = _map.Length - 1; i >= lowerLimit; i--)
                {
                    var th = i < 0 ? 0 : _map[i].MinDotProduct;
                    var isLastBody = i == lowerLimit || th <= minDotProduct;

                    var bestDp = float.MinValue;
                    var bestId = -1;
                    var clusters = th <= float.Epsilon
                        ? GetNearbyVectorsExhaustiveStrategy(vector)
                        : GetNearbyVectorsCountStrategy(_map[i], vector, countDict, offset);
                    foreach (var id in clusters)
                    {
                        var dp = _indexedVectors[id].DotProductWith(vector);
                        if (dp <= bestDp)
                            continue;
                        bestDp = dp;
                        bestId = id;
                    }

                    if (bestDp < th)
                    {
                        if (isLastBody)
                            return (-1, default);
                        continue;
                    }

                    return (bestId, bestDp);
                }

                return (-1, default);
            }
            finally
            {
                if (countDict != EmptyDict)
                    _vectorDictionaryBag.Add(countDict);
            }


        }


        /// <summary>
        /// Retrieve k nearest neighbors from index (can be fewer than k), sorted in descending order of similarity.
        /// Neighbors must have dot product > 0 due to the inner workings of this indexing structure.
        /// A threshold > 0 (<paramref name="minDotProduct"/>) can be defined
        /// to ignore neighbors that are too far away.
        /// </summary>
        /// <param name="vector">The vector whose neighbors should be retrieved</param>
        /// <param name="k">The number of neighbors to retrieve</param>
        /// <param name="minDotProduct">Threshold to ignore distant vectors (actual dot product may still be lower)</param>
        /// <returns>Corresponding identifiers of the k (or fewer) nearest neighbors.</returns>
        /// <exception cref="ArgumentException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public int[] GetKNearestVectors(FlexibleVector vector, int k, float minDotProduct = 0)
        {
            if (minDotProduct < 0)
                throw new ArgumentException("indexing structure does not support negative dot product threshold");
            if (k <= 0 || vector.Length == 0)
                return Array.Empty<int>();

            if (_intListBag.TryTake(out var vecList))
                vecList.Clear();
            else
                vecList = new List<int>(k);
            var countDict = EmptyDict;
            //count occurrences with global map first so that we do not need to repeat this for every threshold
            int offset = 0;
            if (!_hasOnlyZeroThreshold)
                countDict = CountAffectedClusters(vector.Indexes, out offset);
            try
            {
                var lowerLimit = _map[0].MinDotProduct <= float.Epsilon ? 0 : -1;
                for (int i = _map.Length - 1; i >= lowerLimit; i--)
                {
                    var th = i < 0 ? 0 : _map[i].MinDotProduct;
                    var isLastBody = i == lowerLimit || th <= minDotProduct;
                    vecList.Clear();
                    var clusters = th <= float.Epsilon
                        ? GetNearbyVectorsExhaustiveStrategy(vector)
                        : GetNearbyVectorsCountStrategy(_map[i], vector, countDict, offset);
                    foreach (var id in clusters)
                    {
                        vecList.Add(id);
                    }
                    if (!isLastBody && vecList.Count < k)
                        continue;
                    if (vecList.Count == 0)
                        return Array.Empty<int>();
                    var cosDistanceList = ArrayPool<float>.Shared.Rent(vecList.Count);
                    var numAboveTh = 0;
                    if (vector.Length * vecList.Count > 10_000)
                    {
                        Parallel.For(0, vecList.Count, j =>
                        {
                            var dp = vector.DotProductWith(_indexedVectors[vecList[j]]);
                            cosDistanceList[j] = 1 - dp;
                            if (dp >= th)
                                Interlocked.Increment(ref numAboveTh);
                        });
                    }
                    else
                    {
                        for (int j = 0; j < vecList.Count; j++)
                        {
                            var dp = vector.DotProductWith(_indexedVectors[vecList[j]]);
                            cosDistanceList[j] = 1 - dp;
                            if (dp >= th)
                                numAboveTh++;
                        }
                    }
                    

                    if (!isLastBody && numAboveTh < k)
                    {
                        //cannot guarantee that entries below th belong to the nearest neighbors
                        ArrayPool<float>.Shared.Return(cosDistanceList);
                        continue;
                    }

                    var numRes = Math.Min(k, numAboveTh);
                    var res = new int[numRes];

                    if (numRes == 1)
                    {
                        //do not need to sort results, we can take best item
                        var bestDistance = float.MaxValue;
                        for (int j = 0; j < vecList.Count; j++)
                        {
                            var distance = cosDistanceList[j];
                            if (distance >= bestDistance)
                                continue;
                            bestDistance = distance;
                            res[0] = vecList[j];
                        }
                    }
                    else if (numRes > 1)
                    {
                        var idList = ArrayPool<int>.Shared.Rent(vecList.Count);
                        vecList.CopyTo(idList);
                        Array.Sort(cosDistanceList, idList, 0, vecList.Count);
                        Array.Copy(idList, res, res.Length);
                        ArrayPool<int>.Shared.Return(idList);
                    }


                    ArrayPool<float>.Shared.Return(cosDistanceList);
                    return res;
                }

                return Array.Empty<int>();
            }
            finally
            {
                _intListBag.Add(vecList);
                if (countDict != EmptyDict)
                    _vectorDictionaryBag.Add(countDict);
            }
        }



        /// <summary>
        /// Retrieve all indexed vector ids that can lead to a dot product >= MinDotProduct with the query <paramref name="vector"/>.
        /// Only valid if the provided <paramref name="vector"/> has length equal to or smaller than 1.
        /// Actual dot product can also be smaller.
        /// </summary>
        /// <param name="vector">The query vector</param>
        /// <returns>Enumeration of found vector IDs</returns>
        public IEnumerable<int> GetNearbyVectors(FlexibleVector vector)
        {
            return GetNearbyVectors(vector, MinDotProduct);
        }


        /// <summary>
        /// Retrieve all indexed vector ids that can lead to a dot product >= <param name="minDotProduct"></param> with the query <paramref name="vector"/>.
        /// Only valid if provided <paramref name="vector"/> has length equal to or smaller than 1.
        /// Actual dot product can also be smaller.
        /// </summary>
        /// <param name="vector">the query vector</param>
        /// <param name="minDotProduct">the lower bound of the dot product, has to be greater or equal 0</param>
        /// <returns>Enumeration of found vector IDs</returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public IEnumerable<int> GetNearbyVectors(FlexibleVector vector, float minDotProduct)
        {
            if (!vector.IsSparse)
                vector = vector.ToSparse();

            if (minDotProduct < 0)
                throw new ArgumentException("indexing structure does not support negative dot product threshold");

            if (vector.Length == 0)
                return Enumerable.Empty<int>();

            if (minDotProduct <= float.Epsilon ||
                minDotProduct < _map[0].MinDotProduct ||
                _hasOnlyZeroThreshold)
            {
                return vector.Indexes.Length == 1
                    ? GetNearbyVectorsSingleTokenExhaustiveStrategy(vector.Indexes[0])
                    : GetNearbyVectorsExhaustiveStrategy(vector);
            }

            for (int i = _map.Length - 1; i >= 0; i--)
            {
                var map = _map[i];
                if (map.MinDotProduct <= minDotProduct)
                {
                    return vector.Indexes.Length == 1
                        ? GetNearbyVectorsSingleTokenStrategy(map, vector.Indexes[0])
                        : GetNearbyVectorsCountStrategy(map, vector);
                }
            }

            throw new Exception("code path should not be reached");
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private IEnumerable<int> GetNearbyVectorsCountStrategy(DotProductThItem map, FlexibleVector vector,
            Dictionary<int, int>? cachedCountDict = null, int cachedOffset = 0)
        {
            if (vector.Length < 8 || _map.Length == 1 && _map[0].MinDotProduct < 0.2f)
                return GetNearbyVectorsCountStrategyDefault(map, vector, cachedCountDict, cachedOffset);
            return GetNearbyVectorsCountStrategyStaggered(map, vector, cachedCountDict, cachedOffset);
        }
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private IEnumerable<int> GetNearbyVectorsCountStrategyDefault(DotProductThItem map, FlexibleVector vector,
            Dictionary<int, int>? cachedCountDict = null, int cachedOffset = 0)
        {
            if (_vectorHashSetBag.TryTake(out var hashSet))
                hashSet.Clear();
            else
                hashSet = new HashSet<int>();

            var vecLen = vector.Length;
            var indexes = vector.Indexes;
            var countDict = cachedCountDict ?? EmptyDict;
            var offset = cachedOffset;
            
            if (cachedCountDict == null)
                countDict = CountAffectedClusters(indexes, out offset);

            for (int j = 0; j < indexes.Length; j++)
            {
                var idx = indexes[j];
                if (map.TokenToVectorsMap.TryGetValue(idx, out var vecList))
                {
                    for (int i = 0; i < vecList.Count; i++)
                    {
                        var (id, minNumOccurrences) = vecList[i];
                        if (minNumOccurrences == 1 || minNumOccurrences <= vecLen && countDict.GetValueOrDefault(id) >= minNumOccurrences - offset)
                        {
                            hashSet.Add(id);
                        }
                    }
                    if (hashSet.Count == VectorsCount)
                        break; //already all means returned
                }
            }

            foreach (var k in hashSet)
            {
                yield return k;
            }

            if (cachedCountDict == null && countDict != EmptyDict)
            {
                _vectorDictionaryBag.Add(countDict);
            }
            _vectorHashSetBag.Add(hashSet);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private IEnumerable<int> GetNearbyVectorsCountStrategyStaggered(DotProductThItem map, FlexibleVector vector,
            Dictionary<int, int>? cachedCountDict = null, int cachedOffset = 0)
        {
            if (_vectorHashSetBag.TryTake(out var hashSet))
                hashSet.Clear();
            else
                hashSet = new HashSet<int>();

            var origMap = map;
            var origMapIndex = Array.IndexOf(_map, map);
            var nextMapIndex = origMapIndex + 1;
            DotProductThItem? nextMap = null;
            float squaredSumForNextMap = 1f; //if we reach this sum of processed entries we can switch to next map
            float maxSquaredSum = map.MaxSquaredSum; //if we reach this sum of processed entries we cannot reach th anymore
            if (nextMapIndex < _map.Length)
            {
                nextMap = _map[nextMapIndex];
                //if the remaining length falls below this value we can switch to next map
                //since maxPossibleLengthOfInputVec * map.MinDotProduct is the actual dot product threshold
                var reqVectorLen = origMap.MinDotProduct / nextMap.MinDotProduct;
                squaredSumForNextMap = 1 - reqVectorLen * reqVectorLen + float.Epsilon;
            }


            var vecLen = vector.Length;
            var indexes = vector.Indexes;
            var values = vector.Values;
            var countDict = cachedCountDict ?? EmptyDict;
            var offset = cachedOffset;

            if (cachedCountDict == null)
                countDict = CountAffectedClusters(indexes, out offset);

            var squaredSum = 0f;
            for (int j = 0; j < indexes.Length; j++)
            {
                var idx = indexes[j];
                if (map.TokenToVectorsMap.TryGetValue(idx, out var vecList))
                {
                    for (int i = 0; i < vecList.Count; i++)
                    {
                        var (id, minNumOccurrences) = vecList[i];
                        if (minNumOccurrences == 1 || minNumOccurrences <= vecLen && countDict.GetValueOrDefault(id) >= minNumOccurrences - offset)
                        {
                            hashSet.Add(id);
                        }
                    }
                    if (hashSet.Count == VectorsCount)
                        break; //already all means returned
                }

                var val = values[j];
                squaredSum += val*val;
                if (squaredSum > maxSquaredSum)
                    break;
                if (nextMap != null && squaredSum > squaredSumForNextMap)
                {
                    map = nextMap;
                    nextMapIndex++;
                    if (nextMapIndex < _map.Length)
                    {
                        nextMap = _map[nextMapIndex];
                        var reqVectorLen = origMap.MinDotProduct / nextMap.MinDotProduct;
                        squaredSumForNextMap = 1 - reqVectorLen * reqVectorLen + float.Epsilon;
                    }
                    else
                        nextMap = null;
                }
            }

            foreach (var k in hashSet)
            {
                yield return k;
            }

            if (cachedCountDict == null && countDict != EmptyDict)
            {
                _vectorDictionaryBag.Add(countDict);
            }
            _vectorHashSetBag.Add(hashSet);
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private Dictionary<int, int> CountAffectedClusters(ReadOnlySpan<int> indexes, out int offset)
        {
            offset = 0;
            if (_vectorDictionaryBag.TryTake(out var countDict))
                countDict.Clear();
            else
                countDict = new();
            var countTh = Math.Max(3, _indexedVectors.Count / 4);
            for (int j = 0; j < indexes.Length; j++)
            {
                var idx = indexes[j];
                if (_globalMap.TryGetValue(idx, out var l))
                {
                    if (l.Count >= countTh)
                    {
                        //avoid going through long lists, we just set down the threshold for retrieval
                        offset++;
                        continue;
                    }

                    for (int i = 0; i < l.Count; i++)
                    {
                        countDict.IncrementItem(l[i]);
                    }
                }
            }

            return countDict;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private IEnumerable<int> GetNearbyVectorsSingleTokenStrategy(DotProductThItem map, int token)
        {
            if (!map.TokenToVectorsMap.TryGetValue(token, out var l))
                yield break;

            for (int i = 0; i < l.Count; i++)
            {
                var (id, minNumOccurrences) = l[i];
                if (minNumOccurrences > 1)
                    continue;
                yield return id;
            }

        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private IEnumerable<int> GetNearbyVectorsExhaustiveStrategy(FlexibleVector vector)
        {
            if (_vectorHashSetBag.TryTake(out var hashSet))
                hashSet.Clear();
            else
                hashSet = new HashSet<int>();

            for (int j = 0; j < vector.Indexes.Length; j++)
            {
                var idx = vector.Indexes[j];
                if (!_globalMap.TryGetValue(idx, out var list)) continue;

                for (int i = 0; i < list.Count; i++)
                {
                    var k = list[i];
                    hashSet.Add(k);
                }
                if (hashSet.Count == VectorsCount)
                    break; //already all means returned
            }

            foreach (var k in hashSet)
            {
                yield return k;
            }

            _vectorHashSetBag.Add(hashSet);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private IEnumerable<int> GetNearbyVectorsSingleTokenExhaustiveStrategy(int token)
        {
            if (!_globalMap.TryGetValue(token, out var list)) yield break;

            for (int i = 0; i < list.Count; i++)
            {
                yield return list[i];
            }
        }


        /// <summary>
        /// Square n values in a and store result in y
        /// </summary>
        /// <param name="n"></param>
        /// <param name="a"></param>
        /// <param name="y"></param>
        /// <exception cref="Exception"></exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public static unsafe void Square(int n, float* a, float* y)
        {
            var len = n;
            const int vecSize = 8;
            const int blockSize = 4 * vecSize;
            var aPtr = a;
            var yPtr = y;
            if (Avx.IsSupported)
            {
                if (Vector256<float>.Count != vecSize)
                    throw new Exception("Vector256<float>.Count mismatch");
                while (len >= blockSize)
                {
                    var a0 = Avx.LoadVector256(aPtr + 0 * vecSize);
                    var a1 = Avx.LoadVector256(aPtr + 1 * vecSize);
                    var a2 = Avx.LoadVector256(aPtr + 2 * vecSize);
                    var a3 = Avx.LoadVector256(aPtr + 3 * vecSize);
                    Avx.Store(yPtr + 0 * vecSize, Avx.Multiply(a0, a0));
                    Avx.Store(yPtr + 1 * vecSize, Avx.Multiply(a1, a1));
                    Avx.Store(yPtr + 2 * vecSize, Avx.Multiply(a2, a2));
                    Avx.Store(yPtr + 3 * vecSize, Avx.Multiply(a3, a3));
                    yPtr += blockSize;
                    aPtr += blockSize;
                    len -= blockSize;
                }

                while (len >= vecSize)
                {
                    var a0 = Avx.LoadVector256(aPtr);
                    Avx.Store(yPtr, Avx.Multiply(a0, a0));
                    yPtr += vecSize;
                    aPtr += vecSize;
                    len -= vecSize;
                }
            }

            while (len > 0)
            {
                *yPtr = *aPtr * *aPtr;
                yPtr++;
                aPtr++;
                len--;
            }
        }




        private const string StorageMetaId = "dpindex-meta.json";
        private const string StorageBlobId = "dpindex.bin";
        private const string StoragePrefix = "dpindex-";
        private const int StorageBlobSignature = 872149865;

        private class StorageMeta
        {
            public int Version { get; set; } = 1;

            public float MinDotProduct { get; set; }
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
                MinDotProduct = MinDotProduct,
                MaxId = MaxId,
                VectorsCount = VectorsCount,
                Version = 1
            };

            var metaStr = JsonSerializer.Serialize(meta);
            if(zip.Mode == ZipArchiveMode.Update)
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
                w.Write(_map.Length);
                foreach (var m in _map)
                {
                    m.ToWriter(w);
                }
                w.Write(_indexedVectors.Count);
                foreach (var v in _indexedVectors)
                {
                    w.Write(v.Key);
                    v.Value.ToWriter(w);
                }
                w.Write(_globalMap.Count);
                foreach (var key in _globalMap.Keys)
                {
                    w.Write(key);
                    var l = _globalMap[key];
                    w.Write(l.Count);
                    for (int i = 0; i < l.Count; i++)
                    {
                        w.Write(l[i]);
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
                var mapLen = reader.ReadInt32();
                var maps = new DotProductThItem[mapLen];
                for (int i = 0; i < maps.Length; i++)
                {
                    maps[i] = DotProductThItem.FromReader(reader);
                }

                var len = reader.ReadInt32();
                var indexedVectors = new Dictionary<int, FlexibleVector>(len);
                for (int i = 0; i < len; i++)
                {
                    var k = reader.ReadInt32();
                    var v = FlexibleVector.FromReader(reader);
                    indexedVectors.Add(k, v);
                }

                len = reader.ReadInt32();
                var globalMap = new DictListInt<int>();
                globalMap.EnsureDictCapacity(len);
                var tList = new List<int>();
                for (int i = 0; i < len; i++)
                {
                    var k = reader.ReadInt32();
                    var listLen = reader.ReadInt32();
                    tList.Clear();
                    tList.EnsureCapacity(listLen);
                    for (int j = 0; j < listLen; j++)
                    {
                        tList.Add(reader.ReadInt32());
                    }
                    globalMap.AddRange(k, tList);
                }

                return new DotProductIndexedVectors(meta, maps, indexedVectors, globalMap);
            }
            
        }
    }
}
