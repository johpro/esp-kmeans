/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using ESPkMeansLib.Helpers;
using ESPkMeansLib.Model;

namespace ESPkMeansLib
{
    /// <summary>
    /// Multi-threaded implementation of the Euclidean and Spherical k-Means(++) algorithms for clustering dense or sparse data items into k clusters.
    /// </summary>
    public class KMeans
    {
        /// <summary>
        /// Use ++ initialization strategy (default) that seeds initial centroids step-by-step based on distances to already chosen centroids.
        /// Otherwise, choose centroids randomly from data items.
        /// </summary>
        public bool UseKMeansPlusPlusInitialization { get; set; } = true;

        /// <summary>
        /// Enable logging of basic information such as run time
        /// </summary>
        public bool EnableLogging { get; set; }
        /// <summary>
        /// Enable logging of some additional information, including individual steps
        /// </summary>
        public bool EnableVerboseLogging { get; set; }

        /// <summary>
        /// If true uses cosine distance (Spherical k-Means, e.g., better for text). The default is Euclidean distance (traditional k-Means).
        /// </summary>
        public bool UseSphericalKMeans { get; set; }
        /// <summary>
        /// If true (default) uses strategy to reduce number of computations based on centroids that haven't changed between iterations
        /// </summary>
        public bool UseClustersChangedMap { get; set; } = true;
        /// <summary>
        /// If true (default) uses indexing structure to reduce number of computations if input is very sparse. Only works with cosine distance (Spherical k-Means).
        /// </summary>
        public bool UseIndexedMeans { get; set; } = true;
        /// <summary>
        /// Set an optional sampling ratio [0.0 .. 1.0] to perform clustering on a random sample for faster run times (loss in precision). The algorithm will then extrapolate the results to the complete input data set.
        /// </summary>
        public double SamplingRatio { get; set; } = 1;
        /// <summary>
        /// Hyper-parameter that determines when to use differential way of updating cluster centroids (cheaper if there are only a few changes compared to previous iteration)
        /// </summary>
        public int MaxNumChangesForDifferentialUpdate { get; set; } = 1_000;
        /// <summary>
        /// Max-norm threshold to stop optimization when centroids only change marginally between iterations
        /// </summary>
        public double ConvergenceTolerance { get; set; } = 1e-4;

        /// <summary>
        /// Hyper-parameter that determines when to use the indexing structure for Spherical k-Means on sparse data (indexing structure does not pay off if only a handful of centroids change between iterations)
        /// </summary>
        public int MinNumClustersForIndexedMeans { get; set; } = 120;

        private static T[] GetRandomSample<T>(T[] data, double ratio, int minItems = 1)
        {
            var numDataSampled = Math.Max(minItems, (int)(ratio * data.Length));
            if (numDataSampled >= data.Length) return data;

            var newData = new T[numDataSampled];
            var indexes = ArrayPool<int>.Shared.Rent(data.Length);
            for (int i = 0; i < data.Length; i++)
            {
                indexes[i] = i;
            }
            var rnd = new Random();

            for (int i = 0; i < newData.Length; i++)
            {
                var shuffleIdx = rnd.Next(i, data.Length);
                var idx = indexes[shuffleIdx];
                indexes[shuffleIdx] = indexes[i];
                newData[i] = data[idx];
            }
            ArrayPool<int>.Shared.Return(indexes);
            return newData;
        }

        /// <summary>
        /// Cluster dense data items into <paramref name="numClusters"/> clusters and return clustering and cluster centroids.
        /// The data will be converted to an array of FlexibleVector.
        /// The final number of clusters can be lower than <paramref name="numClusters"/> due to redundancies in the data.
        /// If more than one run is requested (<paramref name="numRuns"/> greater than 1), the method returns the best clustering based on the sum of distances between
        /// points and their associated cluster centroid.
        /// </summary>
        /// <param name="data">Input data to cluster</param>
        /// <param name="numClusters">Number of clusters</param>
        /// <param name="numRuns">Number of runs (default is 1 run)</param>
        /// <returns>Clustering (zero-based cluster association of each data item) and cluster centroids</returns>
        /// <exception cref="ArgumentException"></exception>
        public (int[] clustering, FlexibleVector[] clusterMeans) Cluster(IEnumerable<float[]> data, int numClusters,
            int numRuns = 1)
        {
            return Cluster(data.Select(a => new FlexibleVector(a)).ToArray(), numClusters, numRuns);
        }

        /// <summary>
        /// Cluster dense data items into <paramref name="numClusters"/> clusters and return clustering and cluster centroids.
        /// The data will be converted to an array of (float-based) FlexibleVector.
        /// The final number of clusters can be lower than <paramref name="numClusters"/> due to redundancies in the data.
        /// If more than one run is requested (<paramref name="numRuns"/> greater than 1), the method returns the best clustering based on the sum of distances between
        /// points and their associated cluster centroid.
        /// </summary>
        /// <param name="data">Input data to cluster</param>
        /// <param name="numClusters">Number of clusters</param>
        /// <param name="numRuns">Number of runs (default is 1 run)</param>
        /// <returns>Clustering (zero-based cluster association of each data item) and cluster centroids</returns>
        /// <exception cref="ArgumentException"></exception>
        public (int[] clustering, FlexibleVector[] clusterMeans) Cluster(IEnumerable<double[]> data, int numClusters,
            int numRuns = 1)
        {
            return Cluster(data.Select(a => new FlexibleVector(a)).ToArray(), numClusters, numRuns);
        }

        /// <summary>
        /// Cluster sparse data items into <paramref name="numClusters"/> clusters and return clustering and cluster centroids.
        /// The data will be converted to an array of FlexibleVector.
        /// The final number of clusters can be lower than <paramref name="numClusters"/> due to redundancies in the data.
        /// If more than one run is requested (<paramref name="numRuns"/> greater than 1), the method returns the best clustering based on the sum of distances between
        /// points and their associated cluster centroid.
        /// </summary>
        /// <param name="data">Input data to cluster</param>
        /// <param name="numClusters">Number of clusters</param>
        /// <param name="numRuns">Number of runs (default is 1 run)</param>
        /// <returns>Clustering (zero-based cluster association of each data item) and cluster centroids</returns>
        /// <exception cref="ArgumentException"></exception>
        public (int[] clustering, FlexibleVector[] clusterMeans) Cluster(IEnumerable<IEnumerable<(int idx, float value)>> data, int numClusters,
            int numRuns = 1)
        {
            return Cluster(data.Select(a => new FlexibleVector(a)).ToArray(), numClusters, numRuns);
        }

        /// <summary>
        /// Cluster data items into <paramref name="numClusters"/> clusters and return clustering and cluster centroids.
        /// The final number of clusters can be lower than <paramref name="numClusters"/> due to redundancies in the data.
        /// If more than one run is requested (<paramref name="numRuns"/> greater than 1), the method returns the best clustering based on the sum of distances between
        /// points and their associated cluster centroid.
        /// </summary>
        /// <param name="data">Input data to cluster</param>
        /// <param name="numClusters">Number of clusters</param>
        /// <param name="numRuns">Number of runs (default is 1 run)</param>
        /// <returns>Clustering (zero-based cluster association of each data item) and cluster centroids</returns>
        /// <exception cref="ArgumentException"></exception>
        public (int[] clustering, FlexibleVector[] clusterMeans) Cluster(FlexibleVector[] data, int numClusters, int numRuns = 1)
        {
            if (numRuns < 1)
                throw new ArgumentException("invalid number of runs specified");
            
            var isSparse = data[0].IsSparse;
            var dimension = 0;
            if (!isSparse)
                dimension = data[0].Length;
            //sanity checks that input data are in the right format
            foreach (var v in data)
            {
                if (v.IsSparse && !isSparse || !v.IsSparse && isSparse)
                {
                    throw new ArgumentException("data has to have same storage layout, either all dense or all sparse");
                }

                if (!isSparse && dimension != v.Length)
                    throw new ArgumentException($"dense vectors have to be of same size ({dimension}), but got length {v.Length}");
            }

            //this method may be called concurrently on same data array
            if (UseSphericalKMeans)
            {
                EnsureUnitVectors(data);
            }

            if (numRuns == 1)
                return ClusterRun(data, numClusters);

            var bestDistortion = double.MaxValue;
            int[]? bClustering = null;
            FlexibleVector[]? bMeans = null;
            for (int i = 0; i < numRuns; i++)
            {
                var (clustering, means) = ClusterRun(data, numClusters);
                var distortion = EvaluationMetrics.CalculateDistortion(data, clustering, means, UseSphericalKMeans);
                if(bClustering != null && distortion >= bestDistortion)
                    continue;
                bClustering = clustering;
                bMeans = means;
                bestDistortion = distortion;
            }

            return (bClustering, bMeans)!;

        }



        private (int[] clustering, FlexibleVector[] clusterMeans) ClusterRun(FlexibleVector[] data, int numClusters)
        {
            if (data.Length == 0)
                throw new ArgumentException("clustering expects at least one data item");
            if (numClusters < 1)
                throw new ArgumentException("number of clusters has to be >= 1");

            var isSparse = data[0].IsSparse;
            var initialData = data;
            if (SamplingRatio < 1)
            {
                //calculate clustering on a random sample of the data, but return complete, final clustering
                data = GetRandomSample(data, SamplingRatio, numClusters);
            }
            
            bool changed = true; // was there a change in at least one cluster assignment?
            
            DotProductIndexedVectors? indexedMeans = null;
            if (UseSphericalKMeans && UseIndexedMeans && numClusters >= MinNumClustersForIndexedMeans)
                indexedMeans = new DotProductIndexedVectors();


            var watch = Stopwatch.StartNew();

            var initNumClusters = numClusters;
            var clustering = UseKMeansPlusPlusInitialization
                ? InitClusteringPlusPLus(data, ref numClusters)
                : InitClustering(data, ref numClusters);
            
            if (EnableLogging)
                Trace.WriteLine($"InitClustering in {watch.Elapsed} with {numClusters} centroids ({initNumClusters} wanted)"); watch.Restart();

            //sanity check
            /*foreach (var i in clustering)
            {
                if (i < 0 || i >= numClusters)
                    throw new Exception($"numClusters is {numClusters}, but got cluster idx {i}");
            }*/

            

            float[][]? denseMeans = null;

            if (!isSparse)
            {
                denseMeans = new float[numClusters][];
                for (int i = 0; i < denseMeans.Length; i++)
                {
                    denseMeans[i] = new float[data[0].Values.Length];
                }
            }

            var meansWatch = new Stopwatch();
            var updateWatch = new Stopwatch();
            var transformWatch = new Stopwatch();

            var clusterCounts = new int[numClusters];

            FlexibleVector[]? clusterMeans = null;
            Dictionary<int, float>[]? dictMeans = null;
            (int clusterIdxFrom, int clusterIdxTo, int dataIdx)[]? clusteringChanges = null;
            if (isSparse)
            {
                dictMeans = new Dictionary<int, float>[numClusters];
                for (int i = 0; i < dictMeans.Length; i++)
                {
                    dictMeans[i] = new Dictionary<int, float>();
                }

                if (UseSphericalKMeans && MaxNumChangesForDifferentialUpdate > 0)
                {
                    clusteringChanges = ArrayPool<(int, int, int)>.Shared
                        .Rent(Math.Min(Math.Max(1, data.Length / 4), MaxNumChangesForDifferentialUpdate));


                }
            }

            var clustersChangedMap = new List<int>();

            var numDotProductsIndexed = 0;

            var clusterReAssignMap = new int[numClusters];
            var removedClusterIndexes = new List<int>();
            var numChanged = 0;
            int maxCount = data.Length * 10; // sanity check
            int ct = 0;
            while (changed && ct < maxCount)
            {
                ++ct; // k-means typically converges very quickly
                meansWatch.Start();

                FlexibleVector[] newClusterMeans;

                if (isSparse)
                {
                    if (clusteringChanges != null && clusterMeans != null && UseSphericalKMeans && numChanged > 0 &&
                        numChanged <= clusteringChanges.Length && numChanged <= MaxNumChangesForDifferentialUpdate)
                    {
                        newClusterMeans = MeanCalculations.GetMeansUsingChanges(data, clusterCounts, dictMeans!,
                            clusterMeans, clusteringChanges.AsSpan(0, numChanged));
                    }
                    else
                    {
                        (newClusterMeans, clusterCounts) = MeanCalculations.GetMeans(data, numClusters, clustering,
                            UseSphericalKMeans, dictMeans!);
                    }
                }
                else
                {
                    UpdateMeansDense(data, clustering, clusterCounts, denseMeans!, UseSphericalKMeans);
                    newClusterMeans = TransformMeans(denseMeans!);
                }

                meansWatch.Stop();

                transformWatch.Start();

                //for euclidean distances, we should never have the problem of getting clusters with zero items
                //after ++ initialization, but for cosine distance this is different as the "mean" and subsequent normalization
                //can lead to redundant clusters

                var numRemoved = 0;
                removedClusterIndexes.Clear();
                for (int i = 0; i < clusterReAssignMap.Length; i++)
                {
                    clusterReAssignMap[i] = -1;
                }
                for (int i = 0; i < clusterCounts.Length; i++)
                {
                    if (clusterCounts[i] != 0)
                        continue;

                    //this cluster made itself redundant, we can remove it
                    if (!isSparse)
                    {
                        RemoveArrayEntry(denseMeans!, i - numRemoved);
                    }
                    else
                    {
                        RemoveArrayEntry(dictMeans!, i - numRemoved);
                    }

                    RemoveArrayEntry(newClusterMeans, i - numRemoved);

                    if (clusterMeans != null)
                        RemoveArrayEntry(clusterMeans, i - numRemoved);

                    removedClusterIndexes.Add(i - numRemoved);

                    numRemoved++;

                    //we have to update cluster ids as well
                    for (int j = i + 1; j < clusterCounts.Length; j++)
                    {
                        clusterReAssignMap[j] = j - numRemoved;
                    }


                }

                if (numRemoved > 0)
                {
                    //we also need to change clusterCounts as it may be reused later on
                    foreach (var i in removedClusterIndexes)
                    {
                        RemoveArrayEntry(clusterCounts, i);
                    }


                    numClusters -= numRemoved;
                    if (!isSparse)
                        Array.Resize(ref denseMeans, numClusters);
                    Array.Resize(ref clusterCounts, numClusters);
                    Array.Resize(ref newClusterMeans, numClusters);

                    if (clusterMeans != null)
                        Array.Resize(ref clusterMeans, numClusters);
                    if (dictMeans != null)
                        Array.Resize(ref dictMeans, numClusters);

                    for (int i = 0; i < clustering.Length; i++)
                    {
                        var clusterIdx = clustering[i];
                        if (clusterIdx < 0 || clusterIdx >= clusterReAssignMap.Length)
                        {
                            Trace.WriteLine($"ERROR clusterIdx is {clusterIdx}, but assign map length is {clusterReAssignMap.Length}");
                            Debugger.Break();
                        }
                        var newId = clusterReAssignMap[clusterIdx];
                        if (newId == -1)
                            continue;

                        clustering[i] = newId;
                    }


                    Array.Resize(ref clusterReAssignMap, numClusters);

                    if (EnableLogging)
                        Trace.WriteLine($"{numRemoved} redundant clusters removed");
                }

                clustersChangedMap.Clear();
                if (UseClustersChangedMap && numRemoved == 0 && clusterMeans != null && isSparse)
                {
                    for (int i = 0; i < newClusterMeans.Length; i++)
                    {
                        if (!clusterMeans[i].ValueEquals(newClusterMeans[i]))
                        {
                            clustersChangedMap.Add(i);
                        }
                    }
                    if (clustersChangedMap.Count == 0)
                        break; //no need to update clustering again

                    if (EnableVerboseLogging)
                        Trace.WriteLine($"{clustersChangedMap.Count} clusters changed");

                    if (clustersChangedMap.Count >= clusterMeans.Length - 1)
                        clustersChangedMap.Clear(); //equivalent to full search anyway
                }

                var hasConverged = false;

                if (numRemoved == 0 && ConvergenceTolerance > 0 && clusterMeans != null)
                {
                    //if frobenius norm of centroid changes very low we can stop process
                    var centerShift = 0d;
                    for (int i = 0; i < newClusterMeans.Length; i++)
                    {
                        centerShift += clusterMeans[i].SquaredEuclideanDistanceWith(newClusterMeans[i]);
                    }

                    if (centerShift <= ConvergenceTolerance)
                    {
                        if (EnableLogging)
                            Trace.WriteLine("convergence due to low shift");
                        hasConverged = true;
                    }
                }

                clusterMeans = newClusterMeans;
                transformWatch.Stop();

                if (clusterMeans.Length <= 1)
                    break; //no need to update cluster with k = 1

                updateWatch.Start();
                //if(UseSphericalKMeans)
                if (indexedMeans != null &&
                    (clustersChangedMap.Count == 0 ||
                     clustersChangedMap.Count >= MinNumClustersForIndexedMeans &&
                     numDotProductsIndexed > 0 &&
                     clustersChangedMap.Count > numDotProductsIndexed))
                    //only use INDEX strategy if number of changed clusters is high enough and other requirements set (spherical etc.)
                    numChanged = UpdateClusteringIndexed(data, clustering, clusterMeans,
                        clustersChangedMap, clusteringChanges, indexedMeans, out numDotProductsIndexed);
                else
                    numChanged = UpdateClustering(data, clustering, clusterMeans, clustersChangedMap, clusteringChanges);
                //else
                //    numChanged = UpdateClusteringElkan(data, clustering, clusterMeans, clustersChangedMap, clusteringChanges, clusterDistances);
                updateWatch.Stop();

                if (hasConverged)
                    break;

                //Trace.WriteLine($"numChanged: {noChanged}");

                changed = numChanged >= 1;

            }

            if (clusteringChanges != null)
                ArrayPool<(int, int, int)>.Shared.Return(clusteringChanges);

            if (initialData != data)
            {
                //we used subset of data, we have to calculate clustering on actual data array
                clustering = GetClustering(initialData, clusterMeans!);
            }

            if (EnableLogging)
                Trace.WriteLine($"UpdateMeans {meansWatch.Elapsed} | UpdateClusters {updateWatch.Elapsed} | transform {transformWatch.Elapsed} | {ct} iterations");


            return (clustering, clusterMeans)!;
        }






        [MethodImpl(MethodImplOptions.AggressiveOptimization)]
        private static void RemoveArrayEntry<T>(T[] arr, int idx)
        {
            if (idx < arr.Length - 1)
                Array.Copy(arr, idx + 1, arr, idx, arr.Length - idx - 1);
        }


        /// <summary>
        /// Ensure that array contains unit-length vectors. If this is not the case,
        /// this method will replace the respective vector in the array with the normalized version.
        /// </summary>
        /// <param name="data"></param>
        /// <exception cref="Exception"></exception>
        public static void EnsureUnitVectors(FlexibleVector[] data)
        {
            var partition = Partitioner.Create(0, data.Length);

            Parallel.ForEach(partition, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    //if (data[i].Indexes.Length == 0)  throw new Exception($"data item at index {i} is zero, but Spherical kMeans does not support zero vectors");

                    var newItem = data[i].ToUnitVector(out var createdNew);
                    if (createdNew)
                        Interlocked.Exchange(ref data[i], newItem);
                }
            });

            Thread.MemoryBarrier();
        }

        private FlexibleVector[] TransformMeans(float[][] means)
        {
            var res = new FlexibleVector[means.Length];
            for (int k = 0; k < res.Length; k++)
            {
                var v = new FlexibleVector(means[k]);
                if (UseSphericalKMeans)
                    v.NormalizeAsUnitVector();
                res[k] = v;
            }

            return res;
        }


        /// <summary>
        /// Draw initial centroids randomly from data items and calculate clustering.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="numClusters"></param>
        /// <returns></returns>
        private int[] InitClustering(FlexibleVector[] data, ref int numClusters)
        {
            if (numClusters <= 1 || data.Length <= 1)
                return new int[data.Length];

            if (data.Length <= numClusters)
            {
                numClusters = data.Length;
                return Enumerable.Range(0, data.Length).ToArray();
            }
            
            var random = new Random();

            var clusterCentroids = new FlexibleVector[numClusters];
            var hashSet = new HashSet<int>();
            for (int i = 0; i < clusterCentroids.Length; i++)
            {
                int idx;
                do
                {
                    idx = random.Next(data.Length);
                } while (!hashSet.Add(idx));
                clusterCentroids[i] = data[idx];
            }
            return GetClustering(data, clusterCentroids);
        }


        private int[] InitClusteringPlusPLus(FlexibleVector[] data, ref int numClusters)
        {
            /*
             * 1 Choose one center uniformly at random among the data points.
               2 For each data point x, compute D(x), the distance between x and the nearest center that has already been chosen.
               3 Choose one new data point at random as a new center, using a weighted probability distribution where a point x is chosen with probability proportional to D(x)2.
               4 Repeat Steps 2 and 3 until k centers have been chosen.
             */
            if (numClusters <= 1 || data.Length <= 1)
                return new int[data.Length];

            var random = new Random();

            var distances = ArrayPool<float>.Shared.Rent(data.Length);
            for (int i = 0; i < distances.Length; i++)
            {
                distances[i] = float.MaxValue;
            }


            var clusterCentroids = new FlexibleVector[numClusters];

            //if every data item is zero, we will only find one centroid and know that there cannot be more than one cluster
         
            var partition = Partitioner.Create(0, data.Length);

            int k = 0;

            for (; k < numClusters; k++)
            {
                if (k == 0)
                {
                    //choose first cluster centroid randomly
                    clusterCentroids[k] = data[random.Next(data.Length)];
                    continue;
                }

                //calculate distances to closest existing centroids

                var totalDistancesSum = 0d;

                var distanceSumThreshold = 0.00001f;


                var newClusterIdx = k - 1;
                var newClusterCentroid = clusterCentroids[newClusterIdx];
                Parallel.ForEach(partition, range =>
                {
                    var distanceSum = 0d;
                    for (int i = range.Item1; i < range.Item2; i++)
                    {
                        var bestDistance = distances[i];
                        var row = data[i];
                        //we only have to calculate distance to new cluster centroid and check
                        //whether it is smaller than existing min distance

                        var dist = UseSphericalKMeans
                        ? row.CosineDistanceWith(newClusterCentroid)
                        : (float)row.SquaredEuclideanDistanceWith(newClusterCentroid);


                        //var dist = row.SquaredEuclideanDistanceWith(newClusterCentroid);

                        dist = Math.Min(bestDistance, dist);

                        distances[i] = dist;
                        distanceSum += dist;
                    }

                    lock (data)
                        totalDistancesSum += distanceSum;
                });

                Thread.MemoryBarrier();



                if (totalDistancesSum <= distanceSumThreshold)
                    break; //we are already fine with this number of clusters


                //choosing next centroid with probability proportional to distance
                var rndVal = random.NextDouble() * totalDistancesSum;
                double cum = 0;
                for (int i = 0; i < data.Length; i++)
                {
                    //determine actual, random index
                    cum += distances[i];
                    if (rndVal < cum || i == data.Length - 1)
                    {
                        clusterCentroids[k] = data[i];
                        break;
                    }
                }


            }

            numClusters = k;
            if(k < clusterCentroids.Length)
                Array.Resize(ref clusterCentroids, k);

            ArrayPool<float>.Shared.Return(distances);

            //initialize clustering map
            var watch = Stopwatch.StartNew();
            var clustering = GetClustering(data, clusterCentroids);
            if (EnableVerboseLogging)
                Trace.WriteLine($"GetClustering after centroids drawn in {watch.Elapsed}");


            return clustering;
        }

        /// <summary>
        /// Get actual clustering (= cluster associations) of the data according to the provided centroids.
        /// The distance measure and strategy is derived from the class instance properties.
        /// </summary>
        /// <param name="data"></param>
        /// <param name="clusterCentroids"></param>
        /// <param name="distances">if a sufficiently sized array is provided, save calculated distances of each data item to the array</param>
        /// <returns></returns>
        public int[] GetClustering(FlexibleVector[] data, FlexibleVector[] clusterCentroids, float[]? distances = null)
        {
            if (data.Length == 0)
                return Array.Empty<int>();
            return UseSphericalKMeans && UseIndexedMeans && data[0].IsSparse && clusterCentroids.Length >= MinNumClustersForIndexedMeans
                ? GetClusteringIndexed(data, clusterCentroids, distances)
                : GetClusteringExhaustive(data, clusterCentroids, distances);
        }

        private int[] GetClusteringExhaustive(FlexibleVector[] data, FlexibleVector[] clusterCentroids, float[]? distances = null)
        {
            if (distances != null && distances.Length < data.Length)
                throw new ArgumentException(
                    $"provided distances Array is too small ({distances.Length}) to hold all distances ({data.Length})");
            var clustering = new int[data.Length];
            var partition = Partitioner.Create(0, data.Length);

            Parallel.ForEach(partition, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    var bestDistance = double.MaxValue;
                    var row = data[i];
                    var clusterId = -1;
                    for (int j = 0; j < clusterCentroids.Length; j++)
                    {
                        var centroid = clusterCentroids[j];
                        var newDistance = UseSphericalKMeans
                            ? row.CosineDistanceWith(centroid)
                            : row.SquaredEuclideanDistanceWith(centroid);
                        if (newDistance < bestDistance)
                        {
                            bestDistance = newDistance;
                            clusterId = j;
                        }
                    }

                    clustering[i] = clusterId;
                    if (distances != null)
                        distances[i] = (float)bestDistance; //row.SquaredEuclideanDistanceWith(clusterCentroids[clusterId]);
                }


                Thread.MemoryBarrier();
            });


            Thread.MemoryBarrier();
            return clustering;
        }

        private int[] GetClusteringIndexed(FlexibleVector[] data, FlexibleVector[] clusterCentroids, float[]? distances = null)
        {
            if (!UseSphericalKMeans)
                throw new Exception("only works with cosine distance");
            if (distances != null && distances.Length < data.Length)
                throw new ArgumentException(
                    $"provided distances Array is too small ({distances.Length}) to hold all distances ({data.Length})");
            var clustering = new int[data.Length];
            var partition = Partitioner.Create(0, data.Length);
            //we can use normal thresholds as we use knn-strategy
            var indexedMeans = new DotProductIndexedVectors();

            for (int i = 0; i < clusterCentroids.Length; i++)
            {
                var c = clusterCentroids[i];
                if (!c.IsSparse)
                    throw new Exception("all vectors have to have the same storage layout (sparse <> dense)");
                c.Tag = i;
                indexedMeans.Add(c, i);
            }

            var firstCluster = clusterCentroids[0];

            Parallel.ForEach(partition, range =>
            {
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    var row = data[i];
                    var (clusterId, bestDistance) = indexedMeans.GetNearestVector(row);

                    if (bestDistance <= 0)
                    {
                        //we do not know for sure that this is really the nearest cluster.
                        //If we have data with negative vector entries we might get
                        //negative dot products. Indexing structure, however, only works
                        //with thresholds > 0
                        clusterId = 0;
                        bestDistance = row.DotProductWith(firstCluster);
                        for (int k = 1; k < clusterCentroids.Length; k++)
                        {
                            var newDistance = row.DotProductWith(clusterCentroids[k]);
                            if (newDistance <= bestDistance) continue;
                            bestDistance = newDistance;
                            clusterId = k;
                        }
                    }

                    
                    clustering[i] = clusterId;
                    if (distances != null)
                        distances[i] = 1 - bestDistance;
                }
                Thread.MemoryBarrier();
            });


            Thread.MemoryBarrier();
            return clustering;
        }

        
        /// <summary>
        /// Count cluster sizes based on provided clustering
        /// </summary>
        /// <param name="clustering">the cluster associations of the data set</param>
        /// <param name="numClusters"></param>
        /// <returns></returns>
        public static int[] GetClusterCounts(int[] clustering, int numClusters)
        {
            var clusterCounts = new int[numClusters];
            for (int i = 0; i < clustering.Length; ++i)
            {
                int cluster = clustering[i];
                ++clusterCounts[cluster];
            }

            return clusterCounts;
        }
        


        internal static void UpdateMeansDense(FlexibleVector[] data, int[] clustering, int[] clusterCounts, float[][] means,
            bool useSpherical)
        {


            if (clusterCounts.Length == 1)
            {
                clusterCounts[0] = clustering.Length;
            }
            else
            {
                Array.Clear(clusterCounts, 0, clusterCounts.Length);
                foreach (var cluster in clustering)
                {
                    ++clusterCounts[cluster];
                }
            }
            
            foreach (var arr in means)
            {
                Array.Clear(arr, 0, arr.Length);
            }

            for (int i = 0; i < data.Length; ++i)
            {
                int cluster = clustering[i];
                var dataArr = data[i].Values;
                var meanArr = means[cluster];
                if (dataArr.Length != meanArr.Length)
                    throw new Exception("vector sizes do not match");
                for (int j = 0; j < dataArr.Length; j++)
                {
                    meanArr[j] += dataArr[j];
                }
            }

            if (useSpherical)
                return; //we do not need to "normalize" means here, because we will normalize them anyway

            for (int k = 0; k < means.Length; ++k)
            {
                var count = clusterCounts[k];
                if (count <= 0)
                    continue;

                var meanArr = means[k];

                for (int i = 0; i < meanArr.Length; i++)
                {
                    meanArr[i] /= count;
                }
            }
        }



        private int UpdateClusteringIndexed(FlexibleVector[] data, int[] clustering,
           FlexibleVector[] means, List<int> clustersChangedMapSrc,
           (int clusterIdxFrom, int clusterIdxTo, int dataIdx)[]? changes, DotProductIndexedVectors indexedMeans,
           out int numDotProductsIndexed)
        {
            if (means.Length <= 1)
            {
                numDotProductsIndexed = 0;
                return 0;
            }

            for (int i = 0; i < means.Length; i++)
            {
                means[i].Tag = i;
            }

            var noChanges = 0;

            var completeMap = Enumerable.Range(0, means.Length).ToArray();
            var clustersChangedMap = clustersChangedMapSrc.Count == 0 ? null : clustersChangedMapSrc.ToArray();
            var clustersChangedMapSet = clustersChangedMap?.ToHashSet();
            var watch = Stopwatch.StartNew();
            indexedMeans.Set(means);
            if (EnableVerboseLogging)
                Trace.WriteLine($"indexed means in {watch.Elapsed}");
            var minMaxSimilarity = indexedMeans.MinDotProduct + 0.0001f;

            var numIndexedBranches = 0;
            var numNonIndexedBranches = 0;
            var numDotProductsIndexedBranches = 0L;
            var numDotProductsNonIndexedBranches = 0L;

            var numMeansDivBy50 = means.Length / 50;

            //process items in batches to improve efficiency of parallelism for higher data set sizes
            var batchSize = Math.Max(1, Environment.ProcessorCount) * 3_000;
            for (int b = 0; b < data.Length; b += batchSize)
            {
                var bEnd = Math.Min(data.Length, b + batchSize);

                var partition = Partitioner.Create(b, bEnd);
                Parallel.ForEach(partition, range =>
                {
                    for (int i = range.Item1; i < range.Item2; ++i)
                    {
                        var curRow = data[i];

                        var prevClusterId = clustering[i];
                        var map = completeMap;

                        var maxSimilarity = curRow.DotProductWith(means[prevClusterId]);
                        var newClusterId = prevClusterId;
                        var checkOnlyChangedClusters = false;

                        if (clustersChangedMapSet != null && !clustersChangedMapSet.Contains(prevClusterId))
                        {
                            map = clustersChangedMap;
                            checkOnlyChangedClusters = true;
                        }

                        var isIndexedBranch = maxSimilarity >= minMaxSimilarity;
                        var useIndex = isIndexedBranch || curRow.Length < numMeansDivBy50;

                        if (useIndex)
                        {
                            var count = 0;
                            foreach (var k in indexedMeans.GetNearbyVectors(curRow, maxSimilarity))
                            {
                                if (k == prevClusterId ||
                                    checkOnlyChangedClusters && !clustersChangedMapSet!.Contains(k))
                                    continue;

                                count++;

                                var similarity = curRow.DotProductWith(means[k]);
                                if (similarity > maxSimilarity)
                                {
                                    maxSimilarity = similarity;
                                    newClusterId = k;
                                }
                            }

                            //if (isIndexedBranch)
                            // {
                            Interlocked.Increment(ref numIndexedBranches);
                            Interlocked.Add(ref numDotProductsIndexedBranches, count);
                            /*}
                            else
                            {
                                Interlocked.Increment(ref numNonIndexedBranches);
                                Interlocked.Add(ref numDotProductsNonIndexedBranches, count);
                            }*/
                        }
                        else
                        {
                            foreach (var k in map!)
                            {
                                if (k == prevClusterId ||
                                    checkOnlyChangedClusters && !clustersChangedMapSet!.Contains(k))
                                    continue;

                                var meansVec = means[k];

                                var similarity = curRow.DotProductWith(meansVec);
                                if (similarity > maxSimilarity)
                                {
                                    maxSimilarity = similarity;
                                    newClusterId = k;
                                }
                            }


                            Interlocked.Increment(ref numNonIndexedBranches);
                            Interlocked.Add(ref numDotProductsNonIndexedBranches, map.Length);
                        }

                        if (newClusterId < 0)
                            throw new Exception("got erroneous newClusterId of " + newClusterId);
                        if (newClusterId != prevClusterId)
                        {
                            var curChangesCount = Interlocked.Increment(ref noChanges);
                            var changesIdx = curChangesCount - 1;
                            if (changes != null && changesIdx < changes.Length)
                            {
                                //we just keep track of changes so that we can calculate means afterwards faster
                                changes[changesIdx] = (prevClusterId, newClusterId, i);
                            }

                            clustering[i] = newClusterId;
                        }
                    }


                    Thread.MemoryBarrier();
                });
            }

            Thread.MemoryBarrier();

            if (EnableVerboseLogging)
                Trace.WriteLine($"{numIndexedBranches / (double)data.Length:P2} items processed with indexing," +
                                $"{numDotProductsIndexedBranches / Math.Max(1, numIndexedBranches)} vs {numDotProductsNonIndexedBranches / Math.Max(1, numNonIndexedBranches)}  dot p. indexed / non-indexed ," +
                                //$"{FlexibleVector.NumEntriesNull} null vs. {FlexibleVector.NumEntriesNonNull} non-null," +
                                $" total duration {watch.Elapsed}");
            //Trace.WriteLine($"{noChanges} total changes, {hasClusterChanged.Count(b => b)} / {hasClusterChanged.Length} clusters changed");
            numDotProductsIndexed = (int)(numIndexedBranches == 0
                ? means.Length
                : numDotProductsIndexedBranches / numIndexedBranches);
            return noChanges;
        }

        private int UpdateClustering(FlexibleVector[] data, int[] clustering,
           FlexibleVector[] means, List<int> clustersChangedMapSrc,
           (int clusterIdxFrom, int clusterIdxTo, int dataIdx)[]? changes)
        {
            if (means.Length <= 1)
            {
                return 0;
            }

            var noChanges = 0;

            var completeMap = Enumerable.Range(0, means.Length).ToArray();
            var clustersChangedMap = clustersChangedMapSrc.Count == 0 ? null : clustersChangedMapSrc.ToArray();

            var watch = Stopwatch.StartNew();

            //process items in batches to improve efficiency of parallelism for higher data set sizes
            var batchSize = Math.Max(1, Environment.ProcessorCount) * 3_000;
            for (int b = 0; b < data.Length; b += batchSize)
            {
                var bEnd = Math.Min(data.Length, b + batchSize);

                var partition = Partitioner.Create(b, bEnd);
                Parallel.ForEach(partition, range =>
                {
                    for (int i = range.Item1; i < range.Item2; ++i)
                    {
                        float minDistance = float.MaxValue;
                        var newClusterId = -1;
                        var curRow = data[i];

                        var prevClusterId = clustering[i];
                        var map = completeMap;

                        if (clustersChangedMap != null &&
                            clustersChangedMap.IndexOfValueInSortedArray(prevClusterId) == -1)
                        {
                            map = clustersChangedMap;
                            var meansVec = means[prevClusterId];
                            minDistance = UseSphericalKMeans
                                ? curRow.CosineDistanceWith(meansVec)
                                : (float)curRow.SquaredEuclideanDistanceWith(meansVec);
                            newClusterId = prevClusterId;
                        }

                        foreach (var k in map)
                        {
                            var meansVec = means[k];

                            var distance = UseSphericalKMeans
                                ? curRow.CosineDistanceWith(meansVec)
                                : (float)curRow.SquaredEuclideanDistanceWith(meansVec);
                            if (distance < minDistance)
                            {
                                minDistance = distance;
                                newClusterId = k;
                            }
                        }

                        if (newClusterId < 0)
                            throw new Exception("got erroneous newClusterId of " + newClusterId);
                        if (newClusterId != prevClusterId)
                        {
                            var curChangesCount = Interlocked.Increment(ref noChanges);
                            var changesIdx = curChangesCount - 1;
                            if (changes != null && changesIdx < changes.Length)
                            {
                                //we just keep track of changes so that we can calculate means afterwards faster
                                changes[changesIdx] = (prevClusterId, newClusterId, i);
                            }

                            clustering[i] = newClusterId;
                        }
                    }


                    Thread.MemoryBarrier();
                });
            }

            Thread.MemoryBarrier();

            //Trace.WriteLine($"{noChanges} total changes, {hasClusterChanged.Count(b => b)} / {hasClusterChanged.Length} clusters changed");
            if (EnableVerboseLogging)
                Trace.WriteLine($"{noChanges} total changes, elapsed in {watch.Elapsed}");

            return noChanges;
        }

        /*
        /// <summary>
        /// exploit triangle inequality so that we do not need to calculate actual distance to every cluster centroid
        /// </summary>
        /// <param name="data"></param>
        /// <param name="clustering"></param>
        /// <param name="means"></param>
        /// <param name="clustersChangedMapSrc"></param>
        /// <param name="changes"></param>
        /// <param name="clusterDistances"></param>
        /// <returns></returns>
        private int UpdateClusteringElkan(FlexibleVector[] data, int[] clustering,
           FlexibleVector[] means, List<int> clustersChangedMapSrc,
           (int clusterIdxFrom, int clusterIdxTo, int dataIdx)[] changes, float[][] clusterDistances)
        {
            if (UseSphericalKMeans)
                throw new Exception("cosine distance does not meet triangle inequality");

            if (means.Length <= 1)
            {
                return 0;
            }

            var noChanges = 0;

            var completeMap = Enumerable.Range(0, means.Length).ToArray();
            var clustersChangedMap = clustersChangedMapSrc.Count == 0 ? null : clustersChangedMapSrc.ToArray();

            var watch = Stopwatch.StartNew();

            for (int i = 0; i < means.Length; i++)
            {
                var arr = clusterDistances[i];
                var cI = means[i];
                for (int j = i + 1; j < means.Length; j++)
                {
                    if (clustersChangedMap != null &&
                        clustersChangedMap.IndexOfValueInSortedArray(i) == -1 &&
                        clustersChangedMap.IndexOfValueInSortedArray(j) == -1)
                        continue; //both clusters have not changed, do not need to recalculate distance

                    var d = (float)Math.Sqrt(cI.SquaredEuclideanDistanceWith(means[j]));
                    arr[j] = d;
                    clusterDistances[j][i] = d;
                }
            }

            Trace.WriteLine($"calc. cluster distances in {watch.Elapsed}");

            var partition = Partitioner.Create(0, data.Length);
            Parallel.ForEach(partition, range =>
            {
                for (int i = range.Item1; i < range.Item2; ++i)
                {
                    var curRow = data[i];
                    var prevClusterId = clustering[i];
                    var newClusterId = prevClusterId;
                    var prevMeansVec = means[prevClusterId];
                    float minDistance = (float)Math.Sqrt(curRow.SquaredEuclideanDistanceWith(prevMeansVec));

                    var map = completeMap;

                    if (clustersChangedMap != null && clustersChangedMap.IndexOfValueInSortedArray(prevClusterId) == -1)
                    {
                        map = clustersChangedMap;
                    }

                    var curClusterDistances = clusterDistances[prevClusterId];

                    foreach (var k in map)
                    {
                        if (k == prevClusterId)
                            continue;

                        var clusterDist = curClusterDistances[k];
                        if (clusterDist >= 2 * minDistance)
                        {
                            //cluster k cannot be nearer, do not need to calculate distance
                            continue;
                        }

                        var meansVec = means[k];
                        var distance = (float)Math.Sqrt(curRow.SquaredEuclideanDistanceWith(meansVec));
                        if (distance < minDistance)
                        {
                            minDistance = distance;
                            newClusterId = k;
                            curClusterDistances = clusterDistances[k];
                        }
                    }

                    if (newClusterId != prevClusterId)
                    {
                        var curChangesCount = Interlocked.Increment(ref noChanges);
                        if (changes != null && curChangesCount <= changes.Length)
                        {
                            //we just keep track of changes so that we can calculate means afterwards faster
                            changes[curChangesCount - 1] = (prevClusterId, newClusterId, i);
                        }
                        clustering[i] = newClusterId;
                    }
                }


                Thread.MemoryBarrier();
            });

            Thread.MemoryBarrier();

            //Trace.WriteLine($"{noChanges} total changes, {hasClusterChanged.Count(b => b)} / {hasClusterChanged.Length} clusters changed");

            return noChanges;
        }
        */






    }
}