/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
using System.Text.Json;
using System.Text.Json.Serialization;
using ESPkMeansLib.Helpers;

namespace ESPkMeansLib.Model
{
    /// <summary>
    /// Vector that is either backed by a densely populated storage (corresponds to a float array) or by a sparsely populated storage
    /// (list of index-value pairs to store non-zero values, stored as separate lists in Indexes and Values)
    /// </summary>
    public unsafe class FlexibleVector
    {
        /// <summary>
        /// Non-zero indexes if this is a sparse vector
        /// </summary>
        public ReadOnlySpan<int> Indexes => _indexes;

        /// <summary>
        /// vector values (for sparse vectors, only the non-zero values)
        /// </summary>
        public ReadOnlySpan<float> Values => _values;

        /// <summary>
        /// If the vector is sparse, number of non-zero entries.
        /// Otherwise, length of dense vector.
        /// </summary>
        public int Length => _values.Length;

        internal int Tag { get; set; }

        /// <summary>
        /// Returns true if this is a sparse vector that has been sorted with the ToSortedVector() method.
        /// </summary>
        public bool IsSorted
        {
            get
            {
                Thread.MemoryBarrier();
                return _isSorted;
            }
        }

        private readonly int[]? _indexes;

        private readonly float[] _values;

        private volatile bool _isUnitVectorSet;
        private volatile bool _isUnitVector;
        private volatile bool _isSorted;

        /// <summary>
        /// Whether this instance represents a sparse vector
        /// </summary>
        public readonly bool IsSparse;


        private readonly int[] _buckets;
        private readonly int[] _entries;
        private readonly ulong _fastModMult;

        private double _squaredSum;
        private volatile bool _squaredSumSet;

        private float _maxValue;
        private volatile bool _maxValueSet;

        /// <summary>
        /// Sparse zero vector
        /// </summary>
        public static FlexibleVector Zero { get; } = new(Array.Empty<int>(), Array.Empty<float>());

        /// <summary>
        /// Whether this vector has a length of one
        /// </summary>
        public bool IsUnitVector
        {
            get
            {
                if (_isUnitVectorSet)
                    return _isUnitVector;
                _isUnitVector = CheckUnitVector(out _);
                _isUnitVectorSet = true;
                return _isUnitVector;
            }
        }

        [JsonConstructor]
        internal FlexibleVector(int[]? indexes, float[] values) : this(indexes, values, InitBuckets(indexes))
        {
        }

        private FlexibleVector(int[]? indexes, float[] values, (int[] entries, int[] buckets, ulong fastModMult) entriesBuckets)
        {
            _indexes = indexes;
            _values = values;
            if (indexes == null)
            {
                //dense vector
                _entries = Array.Empty<int>();
                _buckets = Array.Empty<int>();
                return;
            }

            //sparse vector
            if (indexes.Length != values.Length)
                throw new Exception("FlexibleVector expects indexes and values arrays with same length");

            IsSparse = true;
            _entries = entriesBuckets.entries;
            _buckets = entriesBuckets.buckets;
            _fastModMult = entriesBuckets.fastModMult;

        }

        /// <summary>
        /// Create sparse vector from index-value pairs
        /// </summary>
        /// <param name="dict"></param>
        public FlexibleVector(Dictionary<int, float> dict)
        {
            IsSparse = true;
            _indexes = new int[dict.Count];
            _values = new float[_indexes.Length];

            switch (dict.Count)
            {
                case 1:
                    {
                        var p = dict.First();
                        _indexes[0] = p.Key;
                        _values[0] = p.Value;
                        break;
                    }
                case > 1:
                    dict.Keys.CopyTo(_indexes, 0);
                    dict.Values.CopyTo(_values, 0);
                    break;
            }
            (_entries, _buckets, _fastModMult) = InitBuckets(_indexes);
        }

        /// <summary>
        /// Create dense vector from array. IMPORTANT: underlying storage will be shared,
        /// i.e., changes to array are reflected here as well, which might lead to wrong or
        /// stale behavior.
        /// </summary>
        /// <param name="denseVector"></param>
        public FlexibleVector(float[] denseVector) : this(null, denseVector)
        {
        }

        /// <summary>
        /// convert dense double array into float-based vector
        /// </summary>
        /// <param name="denseVector"></param>
        public FlexibleVector(double[] denseVector) : this(null, ConvertToFloats(denseVector))
        {
        }

        private static float[] ConvertToFloats(double[] arr)
        {
            var res = new float[arr.Length];
            for (int i = 0; i < res.Length; i++)
            {
                res[i] = (float)arr[i];
            }
            return res;
        }

        /// <summary>
        /// Create sparse vector using list of index-value pairs
        /// </summary>
        /// <param name="items">(index, value) for each non-zero entry of the vector</param>
        public FlexibleVector(IEnumerable<(int idx, float value)> items)
        {
            var list = items as IList<(int idx, float value)> ?? items.ToList();

            _indexes = new int[list.Count];
            _values = new float[_indexes.Length];

            for (int i = 0; i < list.Count; i++)
            {
                _indexes[i] = list[i].idx;
                _values[i] = list[i].value;
            }

            IsSparse = true;

            (_entries, _buckets, _fastModMult) = InitBuckets(_indexes);

        }

        /// <summary>
        /// Create sparse vector from dense vector array, i.e., store only (index,value) pairs that have
        /// non-zero value (threshold can be adjusted)
        /// </summary>
        /// <param name="denseVector">vector array</param>
        /// <param name="epsilon">threshold to determine whether absolute value is zero</param>
        /// <returns></returns>
        public static FlexibleVector CreateSparse(float[] denseVector, float epsilon = float.Epsilon)
        {
            return new FlexibleVector(CreateSparseFromDense(denseVector, epsilon));
        }

        /// <summary>
        /// Create sparse float-based vector from dense double-based vector array, i.e., store only (index,value) pairs that have
        /// non-zero value
        /// </summary>
        /// <param name="denseVector">vector array</param>
        /// <param name="epsilon">threshold to determine whether absolute value is zero</param>
        /// <returns></returns>
        public static FlexibleVector CreateSparse(double[] denseVector, float epsilon = float.Epsilon)
        {
            return new FlexibleVector(CreateSparseFromDense(denseVector, epsilon));
        }

        /// <summary>
        /// Create vector as a frame of provided indexes and values arrays.
        /// This is unsafe as any changes to the arrays will be reflected here as well,
        /// possibly leading to inconsistencies and wrong results.
        /// </summary>
        /// <param name="indexes"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public static FlexibleVector CreateUnsafeReference(int[] indexes, float[] values)
        {
            if (indexes.Length != values.Length)
                throw new ArgumentException("arrays must have matching size");
            return new FlexibleVector(indexes, values);
        }

        private static IEnumerable<(int idx, float val)> CreateSparseFromDense(float[] v, float epsilon)
        {
            for (int i = 0; i < v.Length; i++)
            {
                var val = v[i];
                if (Math.Abs(val) > epsilon)
                    yield return (i, val);
            }
        }
        private static IEnumerable<(int idx, float val)> CreateSparseFromDense(double[] v, float epsilon)
        {
            for (int i = 0; i < v.Length; i++)
            {
                var val = v[i];
                if (Math.Abs(val) > epsilon)
                    yield return (i, (float)val);
            }
        }

        /// <summary>
        /// Get value at index. Will return 0 if entry does not exist.
        /// </summary>
        /// <param name="idx"></param>
        /// <returns></returns>
        public float this[int idx]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
            get
            {
                TryGetValue(idx, out var res);
                return res;

            }
        }

        /// <summary>
        /// Return enumerable to enumerate through (key,value) pairs. Works for sparse and dense vectors.
        /// </summary>
        /// <returns></returns>
        public IEnumerable<(int key, float value)> AsEnumerable()
        {
            if (_indexes == null)
            {
                for (int i = 0; i < _values.Length; i++)
                {
                    yield return (i, _values[i]);
                }

                yield break;
            }
            for (int i = 0; i < _values.Length; i++)
            {
                yield return (_indexes[i], _values[i]);
            }
        }

        /// <summary>
        /// Copy all values to another vector if they have the same size of Values.
        /// Throws an ArgumentException if this is not the case.
        /// </summary>
        /// <param name="other"></param>
        /// <exception cref="ArgumentException"></exception>
        public void CopyValuesTo(FlexibleVector other)
        {
            if (_values.Length != other._values.Length)
                throw new ArgumentException("vectors must have the same size");
            Array.Copy(_values, other._values, _values.Length);
            other.InvalidateValues();
        }


        /// <summary>
        /// For improved efficiency, we introduce our own code for the Index-lookup (for sparse vectors)
        /// and do not make use of the dictionary provided by .net.
        /// This way we can introduce shortcuts because our keys are non-negative integers:
        /// we do not need to call GetHashCode, do not need to mask hash, more
        /// efficient "linked" list because dict will not change, more efficient path if there are no collisions, ...
        /// Even small improvements may have a notable impact on the overall performance
        /// since the TryGetValue() method is a very hot path during clustering
        /// </summary>
        /// <param name="indexes"></param>

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        [MemberNotNull(nameof(_buckets))]
        private static (int[] entries, int[] buckets, ulong fastModMult) InitBuckets(int[]? indexes)
        {
            if (indexes is not { Length: > 1 })
            {
                return (Array.Empty<int>(), Array.Empty<int>(), 0);
            }
            var numBuckets = (uint)HashHelpers.GetPrime(indexes.Length);
            var fastModMult = HashHelpers.GetFastModMultiplier(numBuckets);
            var buckets = new int[numBuckets];

            fixed (int* bucketsPtr = buckets)
            {
                var distinctCount = 0;
                var numHashesWithCollisions = 0;
                foreach (var idx in indexes)
                {
                    var h = bucketsPtr + HashHelpers.FastMod((uint)idx, numBuckets, fastModMult);
                    var prev = *h;
                    if (prev == 0)
                        distinctCount++;
                    else if (prev == -1)
                        numHashesWithCollisions++;
                    *h = prev - 1;
                }
                //Trace.WriteLine($"{numBuckets} buckets");
                //for each index with hash collision we need to store header for respective hash (1) + actual indexes 
                //number of distinct hashes: distinctCount
                //number of hashes that have more than one target ("num groups"): numHashesWithCollisions
                //number of indexes that do not cause collision: distinctCount - numHashesWithCollisions
                //number of indexes that lead to such collisions: indexes.Length - (distinctCount - numHashesWithCollisions)
                var entries = new int[2 * numHashesWithCollisions + indexes.Length - distinctCount + 1];

                fixed (int* entriesPtr = entries, indexesPtr = indexes)
                {
                    var curIdx = 1;
                    for (var i = 0; i < indexes.Length; i++)
                    {
                        var idx = indexesPtr[i];
                        var h = bucketsPtr + HashHelpers.FastMod((uint)idx, numBuckets, fastModMult);
                        var prev = *h;

                        if (prev < 0) //first time we hit this bucket
                        {
                            if (prev == -1)
                            {
                                //no collision, only one target index, we can store it in bucket
                                *h = ~i;
                            }
                            else
                            {

                                *h = curIdx; //set idx to entries array
                                entriesPtr[curIdx] = 1;
                                entriesPtr[curIdx + 1] = i;
                                curIdx += -prev + 1; //reserve block for num entries with same hash + header (length)
                            }

                        }
                        else
                        { //we have already reserved block in entries array
                          //also check that we do not add same index twice

                            //prev now refers to index in entries array with number of items in this bucket
                            //the following entries refer to the index in the indexes array that belong to this bucket
                            var entriesHeader = entriesPtr + prev;
                            var curCount = *entriesHeader + 1;
                            //now first do some sanity checks that the list of indexes is unique
                            var lim = entriesHeader + curCount;
                            for (var ptr = entriesHeader + 1; ptr < lim; ptr++)
                            {
                                if (indexesPtr[*ptr] == idx)
                                    throw new Exception("sparse vector must not have two entries with the same index");
                            }
                            *entriesHeader = curCount;
                            *(entriesHeader + curCount) = i;
                        }
                    }
                }

                return (entries, buckets, fastModMult);
            }
        }

        /// <summary>
        /// Sets value of an entry. For dense vectors, the index corresponds to the actual index of the entry.
        /// For sparse vectors, the index corresponds to the position in the list of non-zero key,value pairs.
        /// Hence, for sparse vectors, only non-zero values can be changed.
        /// </summary>
        /// <param name="arrayIndex"></param>
        /// <param name="val"></param>
        internal void SetArrayValue(int arrayIndex, float val)
        {
            _values[arrayIndex] = val;
            InvalidateValues();
        }

        /// <summary>
        /// Try to set value of entry. For dense vectors, provided index must be in the range of the dense Values array.
        /// For sparse vectors, index must be present in the sparse list of (index,value) pairs. Returns false otherwise.
        /// </summary>
        /// <param name="idx">vector entry index</param>
        /// <param name="val">new value</param>
        /// <returns>Whether value could be set</returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public bool TrySetValue(int idx, float val)
        {
            ref var value = ref TryGetValue(idx);
            if (Unsafe.IsNullRef(ref value))
            {
                return false;
            }

            value = val;
            InvalidateValues();
            return true;
        }

        /// <summary>
        /// Try to get value of entry. Returns false if value does not exist
        /// (either because sparse list of index-value pairs does not have an entry
        /// for that index or because index is out of range of the dense Values array)
        /// </summary>
        /// <param name="idx"></param>
        /// <param name="val"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public bool TryGetValue(int idx, out float val)
        {
            ref var value = ref TryGetValue(idx);
            if (Unsafe.IsNullRef(ref value))
            {
                val = 0;
                return false;
            }

            val = value;
            return true;
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        private ref float TryGetValue(int idx)
        {
            var indexes = _indexes;
            if (indexes == null)
            {
                if ((uint)idx >= (uint)_values.Length)
                    goto NOT_FOUND;
                return ref _values.DangerousGetReferenceAt(idx);
            }
            switch (indexes.Length)
            {
                case 0:
                    goto NOT_FOUND;
                case 1:
                    if (indexes.DangerousGetReferenceAt(0) != idx)
                    {
                        goto NOT_FOUND;
                    }
                    return ref _values.DangerousGetReferenceAt(0);
            }

            return ref TryGetValueHashed(idx, ref _buckets[0], ref _entries[0], ref indexes[0], ref _values[0]);

        NOT_FOUND:
            return ref Unsafe.NullRef<float>();
        }
        

        [MethodImpl(MethodImplOptions.AggressiveOptimization |MethodImplOptions.AggressiveInlining)]
        private ref float TryGetValueHashed(int idx, ref int bucketsPtr, ref int entriesPtr,
            ref int indexesPtr, ref float valuesPtr)
        {
            var mod = HashHelpers.FastMod((uint)idx, (uint)_buckets.Length, _fastModMult);
            //remove bounds check, we already know that mod can never exceed size of _buckets
            //and _buckets is never null
            var entriesIdx = Unsafe.Add(ref bucketsPtr, (int)mod);
            if (entriesIdx == 0)
            {
                goto NOT_FOUND;
            }

            int pos;
            if (entriesIdx < 0)
            {
                //position in array already encoded in entriesIdx as complement
                pos = ~entriesIdx;
                if (Unsafe.Add(ref indexesPtr, pos) != idx)
                {
                    goto NOT_FOUND;
                }
                goto FOUND;
            }
            
            ref var ptrToHeader = ref Unsafe.Add(ref entriesPtr, entriesIdx);
            ref var ptr = ref Unsafe.Add(ref ptrToHeader, 1);
            ref var endPtr = ref Unsafe.Add(ref ptr, ptrToHeader);

            while(Unsafe.IsAddressLessThan(ref ptr, ref endPtr))
            {
                pos = ptr;
                if (Unsafe.Add(ref indexesPtr, pos) == idx)
                    goto FOUND;
                ptr = ref Unsafe.Add(ref ptr, 1);
            }

        NOT_FOUND:
            return ref Unsafe.NullRef<float>();

        FOUND:
            return ref Unsafe.Add(ref valuesPtr, pos);

        }



        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void InvalidateValues()
        {
            _squaredSumSet = false;
            _maxValueSet = false;
            _isUnitVectorSet = false;
            _isSorted = false;
        }

        /// <summary>
        /// For dense vectors, returns length of Values array.
        /// For sparse vectors, returns maximum index position + 1 present in the sparse Indexes list.
        /// </summary>
        /// <returns></returns>
        public int GetMaxVectorLength()
        {
            return _indexes == null ? Values.Length :
                (_indexes.Length == 0 ? 0 : _indexes.Max() + 1);

        }

        /// <summary>
        /// Multiply all present values with provided factor.
        /// </summary>
        /// <param name="val"></param>
        public void MultiplyWith(float val)
        {
            const int vecSize = 8;
            if (Avx.IsSupported && _values.Length >= vecSize)
            {
                if (vecSize != Vector256<float>.Count)
                    throw new Exception("AVX vector size mismatch");

                var factorVec = Vector256.Create(val, val, val, val, val, val, val, val);

                fixed (float* values = _values)
                {
                    var i = 0;
                    var limit = _values.Length - vecSize;

                    for (; i <= limit; i += vecSize)
                    {
                        var v = Avx.LoadVector256(values + i);
                        var product = Avx.Multiply(v, factorVec);
                        Avx.Store(values + i, product);
                    }

                    for (; i < _values.Length; i++)
                    {
                        values[i] *= val;
                    }
                }
            }
            else
            {
                for (int i = 0; i < _values.Length; i++)
                {
                    _values[i] *= val;
                }
            }
            InvalidateValues();
        }

        /// <summary>
        /// Divide all present values
        /// </summary>
        /// <param name="val"></param>
        public void DivideBy(float val)
        {
            const int vecSize = 8;
            if (Avx.IsSupported && _values.Length >= vecSize)
            {
                if (vecSize != Vector256<float>.Count)
                    throw new Exception("AVX vector size mismatch");

                var factorVec = Vector256.Create(val, val, val, val, val, val, val, val);

                fixed (float* values = _values)
                {
                    var i = 0;
                    var limit = _values.Length - vecSize;

                    for (; i <= limit; i += vecSize)
                    {
                        var v = Avx.LoadVector256(values + i);
                        var product = Avx.Divide(v, factorVec);
                        Avx.Store(values + i, product);
                    }

                    for (; i < _values.Length; i++)
                    {
                        values[i] /= val;
                    }
                }
            }
            else
            {
                for (int i = 0; i < _values.Length; i++)
                {
                    _values[i] /= val;
                }
            }
            InvalidateValues();
        }

        /// <summary>
        /// Divide all present values
        /// </summary>
        /// <param name="val"></param>
        public void DivideBy(int val)
        {
            if (val == 0)
                return;
            for (int i = 0; i < _values.Length; i++)
            {
                _values[i] /= val;
            }
            InvalidateValues();
        }

        /// <summary>
        /// Convert dense vector into sparse representation so that only non-zero entries are stored.
        /// Returns same vector if it is already sparse.
        /// </summary>
        /// <param name="epsilon"></param>
        /// <returns></returns>
        public FlexibleVector ToSparse(float epsilon = float.Epsilon)
        {
            return _indexes != null ? this : CreateSparse(_values, epsilon);
        }

        /// <summary>
        /// Convert sparse vector into dense vector. Returns source vector if it is already dense.
        /// </summary>
        /// <param name="size">Dimension of dense vector</param>
        /// <returns></returns>
        public FlexibleVector ToDense(int size)
        {
            if (_indexes == null)
                return this;

            var values = new float[size];
            for (int i = 0; i < _indexes.Length; i++)
            {
                values[_indexes[i]] = _values[i];
            }

            return new FlexibleVector(values);
        }

        /// <summary>
        /// Copy Indexes and Values lists to provided arrays.
        /// </summary>
        /// <param name="indexes"></param>
        /// <param name="values"></param>
        public void CopyTo(int[] indexes, float[] values)
        {
            if (_indexes != null)
                Array.Copy(_indexes, indexes, _indexes.Length);
            Array.Copy(_values, values, _values.Length);
        }
        /// <summary>
        /// Export vector representation into arrays. <paramref name="indexes"/> is null if vector is dense.
        /// </summary>
        /// <param name="indexes"></param>
        /// <param name="values"></param>
        public void ToArrays(out int[]? indexes, out float[] values)
        {
            indexes = _indexes?.ToArray();
            values = _values.ToArray();
        }

        private float SquaredEuclideanDistanceWithDense(FlexibleVector other)
        {
            if (other.IsSparse)
                throw new ArgumentException("both vectors need to be dense (or both sparse)");
            if (other._values.Length != _values.Length)
                throw new ArgumentException("both vectors need to be of same size");

            double sumSquaredDiffs = 0.0;

            const int vecSize = 8;

            if (Avx.IsSupported && _values.Length >= vecSize)
            {
                if (vecSize != Vector256<float>.Count)
                    throw new Exception("AVX vector size mismatch");
                fixed (float* valsA = _values, valsB = other._values)
                {
                    var i = 0;
                    var limit = _values.Length - vecSize;
                    var sumVec = Vector256<float>.Zero;

                    for (; i <= limit; i += vecSize)
                    {
                        var v1 = Avx.LoadVector256(valsA + i);
                        var v2 = Avx.LoadVector256(valsB + i);
                        var diff = Avx.Subtract(v1, v2);

                        //if block should be optimized away
                        if (Fma.IsSupported)
                        {
                            sumVec = Fma.MultiplyAdd(diff, diff, sumVec);
                        }
                        else
                        {
                            var squared = Avx.Multiply(diff, diff);
                            sumVec = Avx.Add(sumVec, squared);
                        }
                    }

                    sumSquaredDiffs += sumVec.GetElement(0)
                                       + sumVec.GetElement(1)
                                       + sumVec.GetElement(2)
                                       + sumVec.GetElement(3)
                                       + sumVec.GetElement(4)
                                       + sumVec.GetElement(5)
                                       + sumVec.GetElement(6)
                                       + sumVec.GetElement(7);

                    for (; i < _values.Length; i++)
                    {
                        var diff = valsA[i] - valsB[i];
                        sumSquaredDiffs += diff * diff;
                    }
                }
            }
            else
            {

                for (int i = 0; i < _values.Length; i++)
                {
                    var diff = _values[i] - other._values[i];
                    sumSquaredDiffs += diff * diff;
                }
            }

            return (float)sumSquaredDiffs;
        }

        /// <summary>
        /// Calculate the squared Euclidean distance to another vector, i.e., |a-b|^2
        /// Both vectors need to have the same storage layout, i.e., both dense or both sparse
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentException"></exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public double SquaredEuclideanDistanceWith(FlexibleVector other)
        {
            if (this == other)
                return 0;

            if (_indexes == null)
                return SquaredEuclideanDistanceWithDense(other);
            if (other._indexes == null)
                throw new ArgumentException("both vectors need to be sparse (or both dense)");

            var a = this;
            var b = other;
            if (other._indexes.Length < _indexes.Length)
            {
                a = other;
                b = this;
            }


            double sumSquaredDiffs = b.GetSquaredSum();
            var aIndexes = a._indexes;

            fixed (float* aVals = a._values)
            {
                //b is bigger vector
                for (var i = 0; i < aIndexes.Length; i++)
                {
                    var idx = aIndexes[i];
                    float diff;
                    if (b.TryGetValue(idx, out var val))
                    {
                        //with this statement we do not need to check for b_indexes whether they have already been processed
                        //this is 10 times (!) faster than check in second loop
                        sumSquaredDiffs -= val * val;
                        diff = aVals[i] - val;
                    }
                    else
                    {
                        diff = aVals[i];
                    }

                    sumSquaredDiffs += diff * diff;
                }
            }
            //our approx. might lead to negative values
            return Math.Max(0, sumSquaredDiffs);
        }

        /// <summary>
        /// Get the maximum value of all entries in this vector.
        /// </summary>
        /// <returns></returns>
        public float GetMaxValue()
        {
            if (_maxValueSet)
            {
                Thread.MemoryBarrier();
                return _maxValue;
            }

            lock (this)
            {
                if (_maxValueSet)
                    return _maxValue;

                float maxVal = float.MinValue;
                foreach (var value in _values)
                {
                    if (value > maxVal)
                        maxVal = value;
                }

                _maxValue = maxVal;
                _maxValueSet = true;
                return maxVal;
            }

        }

        /// <summary>
        /// Get the squared sum of this vector.
        /// </summary>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public double GetSquaredSum()
        {
            Thread.MemoryBarrier();
            if (_squaredSumSet)
                return _squaredSum;
            return CalculateSquaredSum();
        }

        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private double CalculateSquaredSum()
        {
            var sum = 0d;
            lock (this)
            {
                if (_squaredSumSet)
                    return _squaredSum;
                const int vecSize = 8;


                if (Avx.IsSupported && _values.Length >= vecSize)
                {
                    if (vecSize != Vector256<float>.Count)
                        throw new Exception("AVX vector size mismatch");

                    fixed (float* vals = _values)
                    {
                        var i = 0;
                        var limit = _values.Length - vecSize;
                        var sumVec = Vector256<float>.Zero;

                        if (Fma.IsSupported)
                        {
                            for (; i <= limit; i += vecSize)
                            {
                                var valsB = Avx.LoadVector256(vals + i);
                                sumVec = Fma.MultiplyAdd(valsB, valsB, sumVec);
                            }
                        }
                        else
                        {
                            for (; i <= limit; i += vecSize)
                            {
                                var valsB = Avx.LoadVector256(vals + i);
                                var squared = Avx.Multiply(valsB, valsB);
                                sumVec = Avx.Add(sumVec, squared);
                            }
                        }


                        sum += sumVec.GetElement(0)
                               + sumVec.GetElement(1)
                               + sumVec.GetElement(2)
                               + sumVec.GetElement(3)
                               + sumVec.GetElement(4)
                               + sumVec.GetElement(5)
                               + sumVec.GetElement(6)
                               + sumVec.GetElement(7);

                        for (; i < _values.Length; i++)
                        {
                            var val = vals[i];
                            sum += val * val;
                        }
                    }
                }
                else
                {
                    foreach (var val in _values)
                    {
                        sum += val * val;
                    }
                }
                _squaredSum = sum;
                _squaredSumSet = true;
            }
            return sum;
        }

        /// <summary>
        /// Calculates cosine distance to another vector. Both vectors have to be unit-length vectors and
        /// both vectors need to have the same storage layout, i.e., both dense or both sparse
        /// </summary>
        /// <param name="other"></param>
        /// <returns>cosine distance in the range [0,2]</returns>
        /// <exception cref="Exception"></exception>

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public float CosineDistanceWith(FlexibleVector other)
        {
            if (!IsUnitVector || !other.IsUnitVector)
                throw new Exception("only works with unit vectors");
            return 1 - DotProductWith(other);
        }

        /// <summary>
        /// Calculates cosine distance to another vector. Both vectors have to be unit-length vectors.
        /// Source vector has to have sparse storage layout.
        /// </summary>
        /// <param name="other"></param>
        /// <returns>cosine distance in the range [0,2]</returns>
        /// <exception cref="Exception"></exception>
        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public float CosineDistanceWith(Dictionary<int, float> other)
        {
            if (!IsUnitVector)
                throw new Exception("only works with unit vectors");
            return 1 - DotProductWith(other);
        }

        /// <summary>
        /// Calculate dot product with another vector (is cosine similarity for unit-length vectors).
        /// Both vectors need to have the same storage layout, i.e., both dense or both sparse.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>

        [MethodImpl(MethodImplOptions.AggressiveOptimization | MethodImplOptions.AggressiveInlining)]
        public float DotProductWith(FlexibleVector other)
        {

            return _indexes != null
                ? (other._indexes.Length >= _indexes.Length
                    ? InternalSparseDotProductWithBigVec(other)
                    : other.InternalSparseDotProductWithBigVec(this))
                : InternalDenseDotProductWith(other);
        }

        /// <summary>
        /// Calculates dot product with sparse vector represented by a dictionary. Source vector has to have sparse storage layout.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public float DotProductWith(Dictionary<int, float> other)
        {
            double sumAb = 0.0;

            var aArr = _indexes;
            if (aArr == null)
                throw new Exception("This vector has to be sparse.");
            fixed (float* valPtr = _values)
                for (var a = 0; a < aArr.Length; a++)
                {
                    var aIdx = aArr[a];
                    if (!other.TryGetValue(aIdx, out var otherVal))
                        continue;
                    sumAb += valPtr[a] * otherVal;
                }

            return (float)sumAb;
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private float InternalSparseDotProductWithBigVec(FlexibleVector other)
        {
            var aArr = _indexes;
            if (aArr!.Length == 0)
                return 0;
            //we need this switch as we must not call pointer-based TryGetValueHashed overload if bucket size is 0
            switch (other.Indexes.Length)
            {
                case 0:
                    return 0;
                case 1:
                    //aArr must also have exactly length 1 since this method was called so that other is bigger or same-sized vector
                    if (other.Indexes[0] != aArr[0])
                        return 0;
                    return _values.DangerousGetReferenceAt(0) * other._values.DangerousGetReferenceAt(0);
            }

            if (other._buckets.Length == 0)
                throw new InvalidOperationException("length of buckets must be greater than zero");
            double sumAb = 0.0;

            ref var valuesPtr = ref _values.DangerousGetReferenceAt(0);
            ref var otherValuesPtr = ref other._values[0];
            ref var otherIndexesPtr = ref other._indexes![0];
            ref var otherBucketsPtr = ref other._buckets[0];
            ref var otherEntriesPtr = ref other._entries[0];
            for (var a = 0; a < aArr.Length; a++)
            {
                var aIdx = aArr[a];
                ref var otherVal = ref other.TryGetValueHashed(aIdx, ref otherBucketsPtr, ref otherEntriesPtr,
                    ref otherIndexesPtr, ref otherValuesPtr);
                if (Unsafe.IsNullRef(ref otherVal))
                    continue;
                sumAb += Unsafe.Add(ref valuesPtr, a) * otherVal;
            }
            return (float)sumAb;
        }





        private float InternalDenseDotProductWith(FlexibleVector other)
        {
            double sumAb = 0.0;

            if (_indexes != null || other._indexes != null)
                throw new Exception("only dense<>dense or sparse<>sparse supported");

            if (_values.Length != other._values.Length)
                throw new Exception("vectors do not have the same size");

            const int vecSize = 8;

            if (Avx.IsSupported && _values.Length >= vecSize)
            {
                if (vecSize != Vector256<float>.Count)
                    throw new Exception("AVX vector size mismatch");
                fixed (float* valsA = _values, valsB = other._values)
                {
                    var i = 0;
                    var limit = _values.Length - vecSize;
                    var sumVec = Vector256<float>.Zero;

                    for (; i <= limit; i += vecSize)
                    {
                        var v1 = Avx.LoadVector256(valsA + i);
                        var v2 = Avx.LoadVector256(valsB + i);

                        //if block should be optimized away
                        if (Fma.IsSupported)
                        {
                            sumVec = Fma.MultiplyAdd(v1, v2, sumVec);
                        }
                        else
                        {
                            var squared = Avx.Multiply(v1, v2);
                            sumVec = Avx.Add(sumVec, squared);
                        }
                    }

                    sumAb += sumVec.GetElement(0)
                             + sumVec.GetElement(1)
                             + sumVec.GetElement(2)
                             + sumVec.GetElement(3)
                             + sumVec.GetElement(4)
                             + sumVec.GetElement(5)
                             + sumVec.GetElement(6)
                             + sumVec.GetElement(7);

                    for (; i < _values.Length; i++)
                    {
                        sumAb += valsA[i] * valsB[i];
                    }
                }
            }
            else
            {

                for (int i = 0; i < _values.Length; i++)
                {
                    sumAb += _values[i] * other._values[i];
                }
            }

            return (float)sumAb;
        }





        private bool CheckUnitVector(out float squaredSum)
        {
            if (_values.Length == 0)
            {
                squaredSum = 0;
                return true; //we consider the zero vector also as unit vector to support "empty" data rows. will be similarity of 0 anyway
            }

            squaredSum = (float)GetSquaredSum();
            //deviations up to 0.00002 can easily happen due to rounding losses
            return Math.Abs(squaredSum - 1f) < 0.00002f;
        }

        /// <summary>
        /// Normalize this vector so that it has length 1 (zero-length vectors are left untouched)
        /// </summary>
        public void NormalizeAsUnitVector()
        {
            if (_isUnitVector && _isUnitVectorSet || CheckUnitVector(out var squaredSum))
            {
                _isUnitVector = true;
                _isUnitVectorSet = true;
                return;
            }

            if (Math.Abs(squaredSum) < 0.00000001f)
                return; //cannot normalize zero vector

            var norm = (float)Math.Sqrt(squaredSum);
            DivideBy(norm);
            _isUnitVector = true;
            _isUnitVectorSet = true;
        }

        /// <summary>
        /// Convert vector into new unit-length vector. Returns source vector if it already has length of 1.
        /// </summary>
        /// <returns></returns>
        public FlexibleVector ToUnitVector()
        {
            return ToUnitVector(out _);
        }

        /// <summary>
        /// Convert vector into new unit-length vector. Returns source vector if it already has length of 1.
        /// </summary>
        /// <param name="createdNew">set to true if a new vector was created.</param>
        /// <returns></returns>
        public FlexibleVector ToUnitVector(out bool createdNew)
        {
            if (_isUnitVector && _isUnitVectorSet || CheckUnitVector(out var squaredSum))
            {
                _isUnitVector = true;
                _isUnitVectorSet = true;
                createdNew = false;
                return this;
            }

            if (Math.Abs(squaredSum) < 0.00000001f)
            {
                createdNew = false;
                return this; //cannot normalize zero vector
            }

            createdNew = true;
            var norm = (float)Math.Sqrt(squaredSum);
            var vec = new FlexibleVector(_indexes, _values.ToArray(), (_entries, _buckets, _fastModMult));
            vec.DivideBy(norm);
            vec._isUnitVector = true;
            vec._isUnitVectorSet = true;
            return vec;
        }

        /// <summary>
        /// Returns vector in which the sparse (index, value) entry pairs are sorted in descending order of the value.
        /// Returns current instance if IsSorted is true (method was already called to create this instance).
        /// </summary>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">throws an exception if the vector is dense</exception>
        public FlexibleVector ToSortedVector()
        {
            if (_indexes == null)
                throw new InvalidOperationException("only sparse vectors can be sorted");
            if (_isSorted)
                return this;
            var indexes = _indexes.ToArray();
            var values = _values.ToArray();
            if (indexes.Length > 1)
            {
                Array.Sort(values, indexes);
                Array.Reverse(indexes);
                Array.Reverse(values);
            }
            var vec = new FlexibleVector(indexes, values);
            vec._isUnitVector = _isUnitVector;
            vec._isUnitVectorSet = _isUnitVectorSet;
            vec._squaredSum = _squaredSum;
            vec._squaredSumSet = _squaredSumSet;
            vec._maxValue = _maxValue;
            vec._maxValueSet = _maxValueSet;
            vec._isSorted = true;
            return vec;
        }

        /// <summary>
        /// returns true if max-norm |this-other| is smaller than 1e-6
        /// </summary>
        /// <param name="other"></param>
        /// <param name="epsilon">the threshold, which is 1e-6 per default</param>
        /// <returns></returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public bool ValueEquals(FlexibleVector? other, float epsilon = 1e-6f)
        {
            if (other == null)
                return false;

            if (_values == other._values)
            {
                return true;
            }

            if (_indexes != null && other._indexes == null)
                return other.ValueEquals(this);

            if (_indexes == null)
            {
                if (other._indexes == null)
                {
                    //both are dense
                    if (_values.Length != other._values.Length)
                        return false;

                    fixed (float* otherValues = other._values)
                        for (int i = 0; i < _values.Length; i++)
                        {
                            if (Math.Abs(_values[i] - otherValues[i]) > epsilon)
                                return false;
                        }

                    return true;
                }

                //this is dense, other is sparse

                for (int i = 0; i < _values.Length; i++)
                {
                    if (Math.Abs(_values[i] - other[i]) > epsilon)
                        return false;
                }


                for (int i = 0; i < other._indexes.Length; i++)
                {
                    var idx = other._indexes[i];
                    if (idx >= 0 && idx < _values.Length)
                        continue;//already tested
                    if (Math.Abs(other._values[i]) > epsilon)
                        return false;
                }

                return true;
            }

            //both are sparse

            var numEntriesFoundInB = 0;
            fixed (float* values = _values)
            {
                for (int i = 0; i < _indexes.Length; i++)
                {
                    var valA = values[i];
                    if (other.TryGetValue(_indexes[i], out var valB))
                        numEntriesFoundInB++;

                    if (Math.Abs(valA - valB) > epsilon)
                        return false;
                }
            }

            if (numEntriesFoundInB == other._indexes!.Length)
                return true;
            //now test entries from B in A cause we have missed some in B

            fixed (float* values = other._values)
            {
                for (int i = 0; i < other._indexes.Length; i++)
                {
                    if (TryGetValue(other._indexes[i], out _))
                        continue; //we checked this already
                    if (Math.Abs(values[i]) > epsilon)
                        return false;
                }
            }

            return true;
        }

        /// <summary>
        /// Convert vector to dictionary (index,value pairs)
        /// </summary>
        /// <param name="minCapacity"></param>
        /// <returns></returns>
        public Dictionary<int, float> ToDictionary(int minCapacity = 0)
        {
            var res = new Dictionary<int, float>(Math.Max(minCapacity, Values.Length));
            foreach ((int key, float value) in AsEnumerable())
            {
                res.Add(key, value);
            }
            return res;
        }

        /// <summary>
        /// Clone this vector.
        /// </summary>
        /// <returns></returns>
        public FlexibleVector Clone()
        {
            return new(_indexes?.ToArray(), _values.ToArray(),
                (_entries.ToArray(), _buckets.ToArray(), _fastModMult));
        }

        /// <summary>
        /// Dump vector to file. The file will be compressed as GZIP archive if the specified file name ends with ".gz".
        /// </summary>
        /// <param name="fn">target path</param>
        public void ToFile(string fn)
        {
            ToFile(fn, fn.EndsWith(".gz", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Dump vector to a file.
        /// </summary>
        /// <param name="fn">target path</param>
        /// <param name="useGzipCompression">whether to use GZIP compression</param>
        public void ToFile(string fn, bool useGzipCompression)
        {
            using var w = StorageHelper.GetWriter(fn, useGzipCompression);
            ToWriter(w);
        }

        /// <summary>
        /// Dump vectors to file. The file will be compressed as GZIP archive if the specified file name ends with ".gz".
        /// </summary>
        /// <param name="arr">vectors to dump</param>
        /// <param name="fn">target path</param>
        public static void ToFile(FlexibleVector[] arr, string fn)
        {
            ToFile(arr, fn, fn.EndsWith(".gz", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Dump vectors to a file.
        /// </summary>
        /// <param name="arr">vectors to dump</param>
        /// <param name="fn">target path</param>
        /// <param name="useGzipCompression">whether to use GZIP compression</param>
        public static void ToFile(FlexibleVector[] arr, string fn, bool useGzipCompression)
        {
            using var w = StorageHelper.GetWriter(fn, useGzipCompression);
            ToWriter(arr, w);
        }

        /// <summary>
        /// Load vector from dump. If the file name ends with ".gz" the file will be decompressed.
        /// </summary>
        /// <param name="fn"></param>
        /// <returns></returns>
        public static FlexibleVector FromFile(string fn)
        {
            return FromFile(fn, fn.EndsWith(".gz", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Load vector from dump.
        /// </summary>
        /// <param name="fn">file path</param>
        /// <param name="decompressGzip">whether to decompress file</param>
        /// <returns></returns>
        public static FlexibleVector FromFile(string fn, bool decompressGzip)
        {
            using var r = StorageHelper.GetReader(fn, decompressGzip);
            return FromReader(r);
        }


        /// <summary>
        /// Load vectors from dump. If the file name ends with ".gz" the file will be decompressed.
        /// </summary>
        /// <param name="fn"></param>
        /// <returns></returns>
        public static FlexibleVector[] ArrayFromFile(string fn)
        {
            return ArrayFromFile(fn, fn.EndsWith(".gz", StringComparison.OrdinalIgnoreCase));
        }

        /// <summary>
        /// Load vectors from dump.
        /// </summary>
        /// <param name="fn">file path</param>
        /// <param name="decompressGzip">whether to decompress file</param>
        /// <returns></returns>
        public static FlexibleVector[] ArrayFromFile(string fn, bool decompressGzip)
        {
            using var r = StorageHelper.GetReader(fn, decompressGzip);
            return ArrayFromReader(r);
        }

        /// <summary>
        /// Dump vector using provided writer.
        /// </summary>
        /// <param name="writer"></param>
        public void ToWriter(BinaryWriter writer)
        {
            writer.Write(1); //version
            writer.Write(_indexes != null);
            if (_indexes != null)
            {
                writer.Write(_indexes.Length);
                foreach (var idx in _indexes)
                {
                    writer.Write(idx);
                }
            }
            writer.Write(_values != null);
            if (_values != null)
            {
                writer.Write(_values.Length);
                foreach (var v in _values)
                {
                    writer.Write(v);
                }
            }
        }

        /// <summary>
        /// Dump vectors to writer.
        /// </summary>
        /// <param name="arr"></param>
        /// <param name="writer"></param>
        public static void ToWriter(FlexibleVector[] arr, BinaryWriter writer)
        {
            writer.Write(arr.Length);
            foreach (var v in arr)
            {
                v.ToWriter(writer);
            }
        }

        /// <summary>
        /// Create vector from dump using provided reader.
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>

        public static FlexibleVector FromReader(BinaryReader reader)
        {
            var version = reader.ReadInt32();
            int[]? indexes = null;
            float[] values = Array.Empty<float>();
            if (reader.ReadBoolean())
            {
                var len = reader.ReadInt32();
                indexes = new int[len];
                for (int i = 0; i < indexes.Length; i++)
                {
                    indexes[i] = reader.ReadInt32();
                }
            }

            if (reader.ReadBoolean())
            {
                var len = reader.ReadInt32();
                values = new float[len];
                for (int i = 0; i < values.Length; i++)
                {
                    values[i] = reader.ReadSingle();
                }
            }

            return new FlexibleVector(indexes, values);
        }

        /// <summary>
        /// Read vectors from dump using provided reader.
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        public static FlexibleVector[] ArrayFromReader(BinaryReader reader)
        {
            var len = reader.ReadInt32();
            var res = new FlexibleVector[len];
            for (int i = 0; i < res.Length; i++)
            {
                res[i] = FromReader(reader);
            }

            return res;
        }

        /// <summary>
        /// Create vector from JSON string.
        /// </summary>
        /// <param name="json"></param>
        /// <returns></returns>
        public static FlexibleVector FromJson(string json)
        {
            var storage = JsonSerializer.Deserialize<FlexibleVectorStorage>(json);
            return new FlexibleVector(storage.Indexes, storage.Values);
        }

        /// <summary>
        /// Converts vector into a JSON representation.
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        public string ToJson(JsonSerializerOptions? options = null)
        {
            return JsonSerializer.Serialize(new FlexibleVectorStorage(_indexes, _values), options);
        }

        /// <summary>
        /// Writes vector to a Utf8JsonWriter.
        /// </summary>
        /// <param name="writer"></param>
        public void ToJsonWriter(Utf8JsonWriter writer)
        {
            writer.WriteStartObject();
            if (_indexes != null)
            {
                writer.WriteStartArray("Indexes");
                foreach (var index in _indexes)
                {
                    writer.WriteNumberValue(index);
                }
                writer.WriteEndArray();
            }
            writer.WriteStartArray("Values");
            foreach (var val in _values)
            {
                writer.WriteNumberValue(val);
            }
            writer.WriteEndArray();
            writer.WriteEndObject();
        }



        public override string ToString()
        {
            const int numVals = 3;
            if (_indexes == null)
            {
                //is sparse
                return _values.Length <= 2 * numVals
                    ? $"[{ValuesToString(_values)}]"
                    : $"[{ValuesToString(_values.Take(numVals))}, ... , {ValuesToString(_values.Skip(_values.Length - numVals))}]";
            }
            //is dense
            return _values.Length <= 2 * numVals
                ? $"[{ValuesToString(AsEnumerable())}]"
                : $"[{ValuesToString(AsEnumerable().Take(numVals))}, ... , {ValuesToString(_indexes.Skip(_values.Length - numVals).Zip(_values.Skip(_values.Length - numVals)))}]";
        }

        private static string ValuesToString(IEnumerable<float> vals)
        {
            return string.Join(", ", vals.Select(v => v.ToString(CultureInfo.InvariantCulture)));
        }
        private static string ValuesToString(IEnumerable<(int idx, float val)> vals)
        {
            return string.Join(", ", vals.Select(p => $"({p.idx}, {p.val.ToString(CultureInfo.InvariantCulture)})"));
        }


    }

    internal class FlexibleVectorStorage
    {
        public FlexibleVectorStorage(int[]? indexes, float[] values)
        {
            Indexes = indexes;
            Values = values;
        }

        public int[]? Indexes { get; set; }
        public float[] Values { get; set; }
    }

    internal static class HashHelpers
    {

        public const int HashPrime = 101;

        public static readonly int[] Primes = {
            3, 7, 11, 17, 23, 29, 37, 47, 59, 71, 89, 107, 131, 163, 197, 239, 293, 353, 431, 521, 631, 761, 919,
            1103, 1327, 1597, 1931, 2333, 2801, 3371, 4049, 4861, 5839, 7013, 8419, 10103, 12143, 14591,
            17519, 21023, 25229, 30293, 36353, 43627, 52361, 62851, 75431, 90523, 108631, 130363, 156437,
            187751, 225307, 270371, 324449, 389357, 467237, 560689, 672827, 807403, 968897, 1162687, 1395263,
            1674319, 2009191, 2411033, 2893249, 3471899, 4166287, 4999559, 5999471, 7199369 };

        public static bool IsPrime(int candidate)
        {
            if ((candidate & 1) != 0)
            {
                int limit = (int)Math.Sqrt(candidate);
                for (int divisor = 3; divisor <= limit; divisor += 2)
                {
                    if ((candidate % divisor) == 0)
                        return false;
                }
                return true;
            }
            return (candidate == 2);
        }

        public static int GetPrime(int min)
        {
            if (min < 0)
                throw new ArgumentException();

            foreach (var prime in Primes)
            {
                if (prime >= min)
                    return prime;
            }

            for (int i = (min | 1); i < int.MaxValue; i += 2)
            {
                if (IsPrime(i) && ((i - 1) % HashPrime != 0))
                    return i;
            }
            return min;
        }


        public static ulong GetFastModMultiplier(uint divisor) =>
            ulong.MaxValue / divisor + 1;

        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public static uint FastMod(uint value, uint divisor, ulong multiplier)
        {
            return (uint)(((((multiplier * value) >> 32) + 1) * divisor) >> 32);
        }

    }
}