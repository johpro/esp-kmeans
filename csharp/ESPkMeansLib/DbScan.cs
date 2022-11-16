/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using ESPkMeansLib.Helpers;
using ESPkMeansLib.Model;

namespace ESPkMeansLib
{
    /// <summary>
    /// Implements the DBSCAN (Density-Based Spatial Clustering of Applications with Noise)
    /// clustering algorithm for sparse and dense data using different distance metrics.
    /// However, in its current state it is only optimized for clustering sparse vectors using the cosine distance (e.g., text representations)
    /// since it uses DotProductIndexedVectors for retrieving neighborhoods efficiently. Otherwise,
    /// the current implementation falls back to a slow brute force approach for determining neighboring points.
    /// </summary>
    public class DbScan
    {
        /// <summary>
        /// Minimum neighborhood size for a point to be considered a core point
        /// </summary>
        public int MinNumSamples { get; set; } = 5;

        /// <summary>
        /// Maximum distance (based on selected distance method) between neighboring points
        /// </summary>
        public float MaxDistance { get; set; } = 0.5f;

        /// <summary>
        /// Distance measure to be used.
        /// Note: At the moment, a fast implementation is only available
        /// for the cosine distance on sparse vectors with max. cosine distance lower than 1.
        /// In other cases, a slow brute force approach is
        /// used to determine neighborhoods.
        /// </summary>
        public DistanceMethod DistanceMethod { get; set; } = DistanceMethod.Cosine;

        /// <summary>
        /// Enable logging of basic information such as run time
        /// </summary>
        public bool EnableLogging { get; set; }

        public (int[] clustering, int[] clusterCounts) Cluster(FlexibleVector[] data)
        {
            if (data.Length == 0)
                return (Array.Empty<int>(), Array.Empty<int>());

            var isSparse = data[0].IsSparse;
            var dimension = 0;
            if (!isSparse)
                dimension = data[0].Length;
            //sanity checks that input data are in the right format
            var useDpIndex = DistanceMethod == DistanceMethod.Cosine
                             && isSparse && MaxDistance < 1;
            foreach (var v in data)
            {
                if (v.IsSparse && !isSparse || !v.IsSparse && isSparse)
                {
                    throw new ArgumentException("data has to have same storage layout, either all dense or all sparse");
                }

                if (!isSparse && dimension != v.Length)
                    throw new ArgumentException($"dense vectors have to be of same size ({dimension}), but got length {v.Length}");
                
            }


            if (DistanceMethod == DistanceMethod.Cosine)
            {
                //clone array since we may change entries
                data = data.ToArray();

                //this should always come last so that the flag remains set
                KMeans.EnsureUnitVectors(data);
            }

            //first step: obtain neighborhood graph
            var watch = Stopwatch.StartNew();
            var neighborsGraph = new List<int>?[data.Length];

            if (useDpIndex)
            {
                GetNeighborsGraphWithDpIndex(data, neighborsGraph);
            }
            else if (DistanceMethod == DistanceMethod.Euclidean)
            {
                GetNeighborsGraphEuclideanBruteForce(data, neighborsGraph);
            }
            else if (DistanceMethod == DistanceMethod.Cosine)
            {
                GetNeighborsGraphCosineBruteForce(data, neighborsGraph);
            }
            else
            {
                throw new ArgumentOutOfRangeException($"{nameof(DistanceMethod)} has unsupported value");
            }

            if (EnableLogging)
            {
                watch.Stop();
                var numCorePoints = neighborsGraph.Count(g => g != null);
                Trace.WriteLine($"{watch.Elapsed} neighborhood graph computed with {numCorePoints} core points and {data.Length-numCorePoints} noise, " +
                                $"{neighborsGraph.Where(g => g != null).Average(g => g.Count):f1} avg neighborhood size");
                watch.Restart();
            }

            //second step: extract and merge clusters based on neighborhoods
            var clustering = new int[data.Length];
            for (int i = 0; i < clustering.Length; i++)
            {
                clustering[i] = -1; //every point is noise at beginning
            }

            var clusterCounts = new List<int>();
            var stack = new Stack<int>();
            for (int i = 0; i < neighborsGraph.Length; i++)
            {
                if (clustering[i] != -1)
                    continue;
                
                if (neighborsGraph[i] == null)
                {
                    //not a core point
                    continue;
                }
                var clusterId = clusterCounts.Count;
                clusterCounts.Add(0);
                ref var clusterCountItem =
                    ref CollectionsMarshal.AsSpan(clusterCounts)[clusterId];
                var j = i;
                while (true)
                {
                    if (clustering[j] == -1)
                    {
                        clustering[j] = clusterId;
                        clusterCountItem++;

                        var neighbors = neighborsGraph[j];
                        if (neighbors != null)
                        {
                            foreach (var id in neighbors)
                            {
                                if (clustering[id] == -1)
                                    stack.Push(id);
                            }
                        }
                    }

                    if (stack.Count == 0)
                        break;
                    j = stack.Pop();
                }
                
            }

            if (EnableLogging)
            {
                Trace.WriteLine($"{watch.Elapsed} actual clustering computed based on graph");
            }

            return (clustering, clusterCounts.ToArray());
        }

        private void GetNeighborsGraphWithDpIndex(FlexibleVector[] data, List<int>?[] neighborsGraph)
        {
            var partitions = Partitioner.Create(0, data.Length);
            var dpIndex = new DotProductIndexedVectors();
            if (data.Any(v => v.Length == 0))
            {
                //cannot add zero vectors to index
                for (int i = 0; i < data.Length; i++)
                {
                    var vec = data[i];
                    if(vec.Length == 0)
                        continue;
                    dpIndex.Add(vec, i);
                }
            }
            else
            {
                dpIndex.Set(data);
            }
            
            Parallel.ForEach(partitions, range =>
            {
                var dpTh = 1 - MaxDistance;
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    var v = data[i];
                    var candidates = dpIndex.GetNearbyVectors(v, dpTh);
                    if (candidates.Count < MinNumSamples)
                        continue; //not a core point, neighborhood not relevant
                    var numNeighbors = 0;
                    foreach ((_, float dotProduct) in candidates)
                    {
                        if (dotProduct < dpTh)
                            continue;
                        numNeighbors++;
                    }

                    if (numNeighbors < MinNumSamples)
                        continue; //not a core point
                    var neighbors = new List<int>(numNeighbors);
                    foreach ((int id, float dotProduct) in candidates)
                    {
                        if (dotProduct < dpTh)
                            continue;
                        neighbors.Add(id);
                    }

                    neighborsGraph[i] = neighbors;
                }
            });
        }

        private void GetNeighborsGraphEuclideanBruteForce(FlexibleVector[] data, List<int>?[] neighborsGraph)
        {
            var partitions = Partitioner.Create(0, data.Length);

            Parallel.ForEach(partitions, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    var v = data[i];
                    var neighbors = new List<int>();
                    var maxDist = MaxDistance;
                    var maxDistSquared = maxDist * maxDist;
                    for (var j = 0; j < data.Length; j++)
                    {
                        var v2 = data[j];
                        var distSquared = v.SquaredEuclideanDistanceWith(v2);
                        if (distSquared > maxDistSquared)
                            continue;
                        neighbors.Add(j);
                    }
                    if(neighbors.Count < MinNumSamples)
                        continue; //not a core point
                    neighborsGraph[i] = neighbors;
                }
            });
        }

        private void GetNeighborsGraphCosineBruteForce(FlexibleVector[] data, List<int>?[] neighborsGraph)
        {
            var partitions = Partitioner.Create(0, data.Length);

            Parallel.ForEach(partitions, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    var v = data[i];
                    var neighbors = new List<int>();
                    var maxDist = MaxDistance;
                    for (var j = 0; j < data.Length; j++)
                    {
                        var v2 = data[j];
                        if(v.CosineDistanceWith(v2) <= maxDist)
                            neighbors.Add(j);
                    }
                    if (neighbors.Count < MinNumSamples)
                        continue; //not a core point
                    neighborsGraph[i] = neighbors;
                }
            });
        }
    }

    public enum DistanceMethod { Euclidean, Cosine }
}
