/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using ESkMeansLib.Model;

namespace ESkMeansLib.Helpers
{
    public static class MeanCalculations
    {

        /*
        public static FlexibleVector GetEuclideanMean(FlexibleVector[] data)
        {
            var dict = new Dictionary<int, float>();
            foreach (var vec in data)
            {
                AddToMean(vec, dict);
            }

            var res = new FlexibleVector(dict);
            res.DivideBy(data.Length);

            return res;
        }
        */


        public static (FlexibleVector[] means, int[] clusterCounts) GetMeans(FlexibleVector[] data, int numClusters, int[] clustering,
            bool useSpherical, Dictionary<int, float>[]? means = null)
        {
            //var watch = Stopwatch.StartNew();

            var clusterCounts = new int[numClusters];

            if (!data[0].IsSparse)
            {
                var tmpMeans = new float[numClusters].Select(_ => new float[data[0].Length]).ToArray();
                KMeans.UpdateMeansDense(data, clustering, clusterCounts, tmpMeans, useSpherical);
                return (tmpMeans.Select(m => new FlexibleVector(m)).ToArray(), clusterCounts);
            }

            if (means == null || means.Length < numClusters)
            {
                means = new Dictionary<int, float>[numClusters];
                for (int i = 0; i < means.Length; i++)
                {
                    means[i] = new Dictionary<int, float>();
                }
            }
            else
            {
                foreach (var dict in means)
                {
                    dict.Clear();
                }
            }

            Parallel.For(0, clusterCounts.Length, cluster =>
            {
                var meanArr = means[cluster];

                var count = 0;
                for (int i = 0; i < data.Length; ++i)
                {
                    if (cluster != clustering[i])
                        continue;

                    var dataArr = data[i];
                    AddToMean(dataArr, meanArr);
                    count++;
                }

                lock (clusterCounts)
                    clusterCounts[cluster] = count;
            });

            Thread.MemoryBarrier();

            // var time1 = watch.Elapsed;  watch.Restart();


            var res = new FlexibleVector[numClusters];
            Parallel.For(0, res.Length, k =>
            {
                var dict = means[k];
                var vec = new FlexibleVector(dict);

                if (useSpherical)
                    vec.NormalizeAsUnitVector();
                else
                {
                    var numItems = clusterCounts[k];
                    vec.DivideBy(numItems);
                }

                res[k] = vec;
                Thread.MemoryBarrier();
            });

            Thread.MemoryBarrier();

            //var time2 = watch.Elapsed;
            //Trace.WriteLine($"UpdateMeans {time1+time2}  part1 {time1}  part2 {time2}");

            return (res, clusterCounts);
        }


        internal static FlexibleVector[] GetMeansUsingChanges(FlexibleVector[] data,
            int[] clusterCounts, Dictionary<int, float>[] means, FlexibleVector[] meansVec,
            Span<(int clusterIdxFrom, int clusterIdxTo, int dataIdx)> changes)
        {
            //var watch = new Stopwatch(); watch.Start();

            var clusterChangeModes = new byte[means.Length];

            foreach ((int clusterIdxFrom, int clusterIdxTo, int dataIdx) in changes)
            {
                clusterCounts[clusterIdxFrom]--;
                clusterCounts[clusterIdxTo]++;
                var vec = data[dataIdx];
                clusterChangeModes[clusterIdxFrom] = Math.Max((byte)1, clusterChangeModes[clusterIdxFrom]);
                SubtractFromMean(vec, means[clusterIdxFrom]);


                if (AddToMean(vec, means[clusterIdxTo]))
                {
                    clusterChangeModes[clusterIdxTo] = 2;
                }
                else
                {
                    clusterChangeModes[clusterIdxTo] = Math.Max((byte)1, clusterChangeModes[clusterIdxTo]);
                }

            }


            var res = new FlexibleVector[means.Length];
            Parallel.For(0, res.Length, k =>
            {
                var dict = means[k];
                if (dict.Count == 0)
                {
                    res[k] = new FlexibleVector(new[] { 0 }, new[] { 0f });
                    return;
                }

                var changeMode = clusterChangeModes[k];
                if (changeMode == 0)
                {
                    //this cluster has not changed
                    res[k] = meansVec[k];
                    return;
                }
                /*
                if (changeMode == 1)
                {
                    //this cluster mean vector has not replaced any zero entry with a new non-zero value
                    var keys = meansVec[k].Indexes;
                    var values = meansVec[k].Values.ToArray();
                    for (var i = 0; i < keys.Length; i++)
                    {
                        var idx = keys[i];
                        values[i] = dict[idx];
                    }

                    var vec = new FlexibleVector(keys, values);
                    vec.NormalizeAsUnitVector();

                    res[k] = vec;

                    return;
                }*/

                res[k] = new FlexibleVector(dict);
                res[k].NormalizeAsUnitVector();
            });

            //watch.Stop();

            //Trace.WriteLine($"UpdateMeansUsingChanges " + watch.Elapsed);

            return res;
        }


        /// <summary>
        /// Add vector to dictionary representation of another vector
        /// </summary>
        /// <param name="row"></param>
        /// <param name="mean"></param>
        /// <returns>returns true if the structure of the target was changed and a new key was added</returns>
        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        internal static bool AddToMean(FlexibleVector row, Dictionary<int, float> mean)
        {
            var indexes = row.Indexes;
            var values = row.Values;
            var res = false;
            for (int i = 0; i < indexes.Length; i++)
            {
                var idx = indexes[i];

                ref float value = ref CollectionsMarshal.GetValueRefOrAddDefault(mean, idx, out var exists);
                value += values[i];
                if (!exists)
                    res = true;

                /*
                if (mean.TryGetValue(idx, out var val))
                {
                    mean[idx] = val + values[i];
                }
                else
                {
                    mean.Add(idx, values[i]);
                    res = true;
                }*/
            }
            return res;
        }


        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        internal static void SubtractFromMean(FlexibleVector row, Dictionary<int, float> mean)
        {
            var indexes = row.Indexes;
            var values = row.Values;
            for (int i = 0; i < indexes.Length; i++)
            {
                var idx = indexes[i];
                ref float value = ref CollectionsMarshal.GetValueRefOrNullRef(mean, idx);
                if (Unsafe.IsNullRef(ref value))
                    throw new Exception("tried to subtract entry but index does not exist");
                value -= values[i];

                /*
                if (mean.TryGetValue(idx, out var val))
                {
                    mean[idx] = val - values[i];
                }
                else
                {
                    throw new Exception("tried to subtract entry but index does not exist");
                }*/
            }
        }

    }
}
