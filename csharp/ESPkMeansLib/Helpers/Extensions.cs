/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace ESPkMeansLib.Helpers
{
    public static class Extensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static ref T DangerousGetReferenceAt<T>(this T[] array, int i)
        {
            ref var arrayData = ref MemoryMarshal.GetArrayDataReference(array);
            ref var ri = ref Unsafe.Add(ref arrayData, i);
            return ref ri;
        }



        public static void AddToList<TKey, TVal>(this Dictionary<TKey, List<TVal>> dict, TKey key, TVal val) where TKey : notnull
        {
            ref List<TVal>? list = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out _);
            if (list == null)
            {
                list = new List<TVal>();
            }

            list.Add(val);
        }

        public static void IncrementItem<TKey>(this Dictionary<TKey, int> dict, TKey key) where TKey : notnull
        {
            ref var value = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out _);
            value++;
        }

        public static int IncrementItemAndReturn<TKey>(this Dictionary<TKey, int> dict, TKey key) where TKey : notnull
        {
            ref var value = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out _);
            value++;
            return value;
        }

        public static double AddToItem<TKey>(this Dictionary<TKey, double> dict, TKey key, double value) where TKey : notnull
        {
            ref var v = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out _);
            v += value;
            return v;
        }

        public static float AddToItem<TKey>(this Dictionary<TKey, float> dict, TKey key, float value) where TKey : notnull
        {
            ref var v = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out _);
            v += value;
            return v;
        }

        public static int AddToItem<TKey>(this Dictionary<TKey, int> dict, TKey key, int value) where TKey : notnull
        {
            ref var v = ref CollectionsMarshal.GetValueRefOrAddDefault(dict, key, out _);
            v += value;
            return v;
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        public static unsafe int IndexOfValueInSortedArray(this int[] arr, int val)
        {
            var l = 0;
            var r = arr.Length - 1;

            fixed (int* arrPtr = arr) //this method is a hot path, removing bounds check speeds up things significantly
            {

                while (r >= l)
                {
                    var m = l + ((r - l) >> 1); //r+l could lead to overflow

                    var mVal = arrPtr[m];

                    if (mVal < val)
                    {
                        //is between m and r
                        l = m + 1;
                        continue;
                    }

                    if (mVal > val)
                    {
                        //is between l and m
                        r = m - 1;
                        continue;
                    }

                    return m;
                }
            }

            //not found
            return -1;
        }

    }
}
