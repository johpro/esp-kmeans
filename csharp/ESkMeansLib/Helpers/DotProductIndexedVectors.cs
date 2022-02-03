/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using ESkMeansLib.Model;

namespace ESkMeansLib.Helpers
{
    /// <summary>
    /// Indexing structure for unit-length vectors. Given a minimum dot product of Lambda, find all unit-length vectors that can have dot product of at least Lambda.
    /// </summary>
    public class DotProductIndexedVectors
    {
        
        public float MinDotProduct { get; }
        public int VectorsCount { get; private set; }
        public int MaxId { get; private set; }
        

        class DotProductThItem
        {
            /// <summary>
            /// given a token, get list of affected vectors. For each vector id, we also store
            /// how many tokens in source query need to share non-zero entry with vector such that
            /// we get at least required dot product.
            /// </summary>
            public readonly Dictionary<int, List<(int id, int minNumOccurrences)>> TokenToVectorsMap = new();
            

            public float MinDotProduct;
            public float MinDotProductSquared;
            public float MaxSquaredSum;

            public void Clear()
            {
                foreach (var l in TokenToVectorsMap.Values)
                {
                    l.Clear();
                }
            }


        }

        private readonly bool _hasOnlyZeroThreshold;
        private readonly DotProductThItem[] _map;
        private readonly Dictionary<int, List<int>> _globalMap = new();

        private readonly ConcurrentBag<HashSet<int>> _vectorHashSetBag = new();
        private readonly ConcurrentBag<Dictionary<int, int>> _vectorDictionaryBag = new();
        private static readonly Dictionary<int, int> EmptyDict = new(0);

        public int DebugTotalMissed { get; set; }


        public DotProductIndexedVectors() : this(new[] { 0.1f, 0.25f, 0.4f, 0.6f })
        {
        }

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
                _map[i] = new DotProductThItem
                {
                    MinDotProduct = minDotProduct,
                    MinDotProductSquared = minDotProduct * minDotProduct,
                    MaxSquaredSum = 1 - minDotProduct * minDotProduct
                };
            }
        }

        /// <summary>
        /// Clear index
        /// </summary>
        public void Clear()
        {
            foreach (var dict in _map)
            {
                dict.Clear();
            }
            foreach (var list in _globalMap.Values)
            {
                list.Clear();
            }
            DebugTotalMissed = 0;
            VectorsCount = 0;
            MaxId = 0;
        }


        /// <summary>
        /// Clear index and add all vectors in array to index, using the array position as ID
        /// </summary>
        /// <param name="vectors"></param>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public void Set(FlexibleVector[] vectors)
        {
            Clear();
            for (var i = 0; i < vectors.Length; i++)
            {
                Add(vectors[i], i);
            }
        }

        /// <summary>
        /// Add vector v to index. A unique ID has to be provided, which will be returned on querying the index
        /// (the vectors are not stored).
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
            if (_hasOnlyZeroThreshold)
            {
                //we just add all tokens
                AddVectorZero(v, id);
                return;
            }

            var len = SortValues(v, out var indexes, out var values);
            var valuesSquared = ArrayPool<float>.Shared.Rent(len);
            fixed (float* valuesSquaredPtr = valuesSquared, valuesPtr = values)
            {
                Square(len, valuesPtr, valuesSquaredPtr);
            }
            AddWithCountStrategy(indexes.AsSpan(0, len),
                values.AsSpan(0, len), valuesSquared.AsSpan(0, len), id);
            ArrayPool<int>.Shared.Return(indexes);
            ArrayPool<float>.Shared.Return(values);
            ArrayPool<float>.Shared.Return(valuesSquared);
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private void AddVectorZero(FlexibleVector v, int id)
        {
            //will be called if only one Lambda is present and it is 0 -> simple index
            var map = _map[0];
            for (int i = 0; i < v.Indexes.Length; i++)
            {
                var idx = v.Indexes[i];
                map.TokenToVectorsMap.AddToList(idx, (id, 1));
                //map.SingleTokenToVectorsMap?.AddToList(idx, id);
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
            for (int i = 0; i < values.Length; i++)
            {
                var val = values[i];
                if (val < 0)
                    values[i] = Math.Abs(val);
            }
            Array.Sort(values, indexes, 0, len);
            return len;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private void AddWithCountStrategy(Span<int> indexes, Span<float> values, Span<float> valuesSquared, int id)
        {
            for (var i = 0; i < indexes.Length; i++)
            {
                var idx = indexes[i];
                _globalMap.AddToList(idx, id);
            }

            foreach (var map in _map)
            {
                var currentEndIdxOfSumSpan = values.Length - 1;
                var squaredSum = 0f;
                var squaredSumOfSumSpan = 0f;

                var minDotProduct = map.MinDotProduct;
                var requiredSquaredSum = map.MinDotProductSquared - 0.001f;
                var maxSquaredSum = map.MaxSquaredSum + 0.001f; //epsilon so that loop that adds tokens does not stop early

                var minNumTokens = 1;

                for (int i = values.Length - 1; i >= 0; i--)
                {
                    var val = values[i];
                    var idx = indexes[i];
                    var valSquared = valuesSquared[i];
                    squaredSum += valSquared;
                    if (val >= minDotProduct)
                    {
                        //if only this token is present in query it could already be enough to get
                        //required dot product
                        map.TokenToVectorsMap.AddToList(idx, (id, 1));
                        currentEndIdxOfSumSpan = i - 1;
                        continue;
                    }

                    if (val <= float.Epsilon)
                        break;

                    //now determine min num of tokens with a non-zero entry in this vector
                    //to retrieve required min dot product, given that none of the previous (higher-valued)
                    //tokens were present -> minNumTokens increases monotonically

                    //-> we can do this similar to a "sliding window" approach so that we avoid squared time complexity
                    if (squaredSumOfSumSpan > 0)
                    {
                        squaredSumOfSumSpan -= valuesSquared[i + 1];
                    }

                    var isDone = false;
                    minNumTokens--;
                    do
                    {
                        if (currentEndIdxOfSumSpan < 0)
                        {
                            //we cannot achieve required sum anymore with this or next tokens
                            isDone = true;
                            break;
                        }
                        squaredSumOfSumSpan += valuesSquared[currentEndIdxOfSumSpan];
                        currentEndIdxOfSumSpan--;
                        minNumTokens++;
                    } while (squaredSumOfSumSpan < requiredSquaredSum);

                    if (isDone)
                        break;

                    map.TokenToVectorsMap.AddToList(idx, (id, minNumTokens));

                    if (squaredSum > maxSquaredSum)
                    {
                        //if(map.MinDotProduct <= 0.11f) Trace.WriteLine($"stopped at pos {i}, processed {values.Length-i}");
                        break; //done, we cannot achieve required sum anymore with next tokens alone
                    }
                }
            }

        }



        /// <summary>
        /// Retrieve such vector ids that can lead to dot product >= MinDotProduct with query <paramref name="vector"/>.
        /// Only valid if provided <paramref name="vector"/> has length equal to or smaller than 1.
        /// Actual dot product can also be smaller.
        /// </summary>
        /// <param name="vector">the query vector</param>
        /// <returns>Enumeration of found vector IDs</returns>
        public IEnumerable<int> GetNearbyVectors(FlexibleVector vector)
        {
            return GetNearbyVectors(vector, MinDotProduct);
        }


        /// <summary>
        /// Retrieve such vector ids that can lead to dot product >= <param name="minDotProduct"></param> with query <paramref name="vector"/>.
        /// Only valid if provided <paramref name="vector"/> has length equal to or smaller than 1.
        /// Actual dot product can also be smaller.
        /// </summary>
        /// <param name="vector">the query vector</param>
        /// <param name="minDotProduct">the lower bound of the dot product</param>
        /// <returns>Enumeration of found vector IDs</returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public IEnumerable<int> GetNearbyVectors(FlexibleVector vector, float minDotProduct)
        {
            if (!vector.IsSparse)
                vector = vector.ToSparse();

            if (minDotProduct < 0)
                throw new ArgumentException("minimum dot product must not be negative");

            if (vector.Length == 0)
                return Enumerable.Empty<int>();

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

            return GetNearbyVectorsExhaustiveStrategy(vector);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private IEnumerable<int> GetNearbyVectorsCountStrategy(DotProductThItem map, FlexibleVector vector)
        {
            if (_vectorHashSetBag.TryTake(out var hashSet))
                hashSet.Clear();
            else
                hashSet = new HashSet<int>();

            var vecLen = vector.Length;

            var countDict = EmptyDict;
            if (!_hasOnlyZeroThreshold)
            {
                //in the case of only one threshold we do not need to count with global map (and global map is also not populated)
                if (_vectorDictionaryBag.TryTake(out countDict))
                    countDict.Clear();
                else
                    countDict = new();

                for (int j = 0; j < vector.Indexes.Length; j++)
                {
                    var idx = vector.Indexes[j];
                    if (_globalMap.TryGetValue(idx, out var list) && list.Count != 0)
                    {
                        foreach (var k in list)
                        {
                            countDict.IncrementItem(k);
                        }
                    }
                }
            }

            for (int j = 0; j < vector.Indexes.Length; j++)
            {
                var idx = vector.Indexes[j];
                if (map.TokenToVectorsMap.TryGetValue(idx, out var vecList) && vecList.Count != 0)
                {
                    foreach (var (id, minNumOccurrences) in vecList)
                    {
                        if (minNumOccurrences == 1 || minNumOccurrences <= vecLen && countDict[id] >= minNumOccurrences)
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
            
            if (countDict != EmptyDict)
            {
                countDict.Clear();
                _vectorDictionaryBag.Add(countDict);
            }

            hashSet.Clear();
            _vectorHashSetBag.Add(hashSet);
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private IEnumerable<int> GetNearbyVectorsSingleTokenStrategy(DotProductThItem map, int token)
        {
            if (!map.TokenToVectorsMap.TryGetValue(token, out var l))
                yield break;
            foreach ((int id, int minNumOccurrences) in l)
            {
                if(minNumOccurrences > 1)
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
                if (_globalMap.TryGetValue(idx, out var list) && list.Count != 0)
                {
                    foreach (var k in list)
                    {
                        if (hashSet.Add(k))
                            yield return k;
                    }

                    if (hashSet.Count == VectorsCount)
                        break; //already all means returned
                }

            }

            hashSet.Clear();
            _vectorHashSetBag.Add(hashSet);
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
    }
}
