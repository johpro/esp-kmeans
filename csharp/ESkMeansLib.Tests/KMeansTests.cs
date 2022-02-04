/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ESkMeansLib;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection.Metadata;
using System.Text;
using System.Threading.Tasks;
using ElskeLib.Utils;
using ESkMeansLib.Helpers;
using ESkMeansLib.Model;
using ESkMeansLib.Tests.datasets;
using ESkMeansLib.Tests.Model;

namespace ESkMeansLib.Tests
{
    [TestClass()]
    public class KMeansTests
    {
        [TestMethod]
        public void ClusterIrisTest()
        {
            var set = TestSet.LoadIris();



            Trace.WriteLine("dense\r\n");

            RunBasicKMeansVariations(set, 3);
            RunBasicKMeansVariations(set, 2);
            RunBasicKMeansVariations(set, 5);

            Trace.WriteLine("\r\nsparse\r\n");

            set.Data = set.Data!.Select(v => v.ToSparse()).ToArray();

            RunBasicKMeansVariations(set, 3);
            RunBasicKMeansVariations(set, 2);
            RunBasicKMeansVariations(set, 5);

        }

        [TestMethod]
        public void ClusterNewsgroupsTest()
        {
            var set = TestSet.Load20Newsgroups();

            RunBasicKMeansVariations(set, 20);
        }

        [TestMethod]
        public void ClusterArxivTest()
        {
            const int numClusters = 100;
            var set = TestSet.LoadArxiv100K();
            var kmeans = new KMeans { UseSphericalKMeans = true, EnableLogging = true };
            var (clustering, centroids) = kmeans.Cluster(set.Data!, numClusters, 5);
            var clusterCounts = KMeans.GetClusterCounts(clustering, numClusters);

            var elske = KeyphraseExtractor.FromFile("datasets/arxiv_100k.elske");

            Trace.WriteLine("\r\n");
            for (int i = 0; i < centroids.Length; i++)
            {
                Trace.WriteLine($"CLUSTER {i} | {clusterCounts[i]} items | {GetClusterDescription(centroids[i], elske)}");
            }

        }

        [TestMethod]
        public void ClusterArxivBenchmarkTest()
        {
            const int numRuns = 2;
            var set = TestSet.LoadArxiv100K();
            var kmeans = new KMeans { UseSphericalKMeans = true };
            foreach (var numClusters in new[]{100, 1_000})
            {
                Trace.WriteLine($"\r\n{numClusters} clusters\r\n");
                kmeans.UseIndexedMeans = false;
                kmeans.UseClustersChangedMap = false;
                var watch = Stopwatch.StartNew();
                kmeans.Cluster(set.Data!, numClusters, numRuns);
                watch.Stop();
                Trace.WriteLine($"{watch.Elapsed / numRuns} | baseline");

                kmeans.UseIndexedMeans = false;
                kmeans.UseClustersChangedMap = true;
                watch.Restart();
                kmeans.Cluster(set.Data!, numClusters, numRuns);
                watch.Stop();
                Trace.WriteLine($"{watch.Elapsed / numRuns} | NCC");

                kmeans.UseIndexedMeans = true;
                kmeans.UseClustersChangedMap = true;
                watch.Restart();
                kmeans.Cluster(set.Data!, numClusters, numRuns);
                watch.Stop();
                Trace.WriteLine($"{watch.Elapsed / numRuns} | NCC+INDEX");
            }
        }

        [TestMethod()]
        public void EnsureUnitVectorsTest()
        {
            var set = FlexibleVectorTests.CreateRandomVectors(1000, true);
            Parallel.For(0, 10, i =>
            {
                KMeans.EnsureUnitVectors(set);
            });
            foreach (var v in set)
            {
                Assert.IsTrue(v.IsUnitVector);
            }
        }

        [TestMethod]
        public void ExamplesTest()
        {

            var km = new KMeans();

            var data = new[]
            {
                new[] { 0.1f, 0.8f },
                new[] { 0.2f, 0.7f },
                new[] { 0.5f, 0.45f },
                new[] { 0.6f, 0.5f }
            };
            //cluster data into two clusters with k-Means++
            var (clustering, centroids) = km.Cluster(data, 2);
            /* OUTPUT:
             * clustering: 0,0,1,1
             * centroids: [[0.15, 0.75], [0.55, 0.475]] */
            Trace.WriteLine($"clustering: {string.Join(',', clustering)}");
            Trace.WriteLine("centroids:");
            foreach (var c in centroids)
                Trace.WriteLine(c);

            //run clustering five times to cluster data into two clusters with k-Means++
            (clustering, centroids) = km.Cluster(data, 2, 5);

            //sparse data specified with index,value pairs
            var sparseData = new[]
            {
                new[] { (0, 0.1f), (3, 0.8f), (7, 0.1f) },
                new[] { (0, 0.2f), (3, 0.8f), (6, 0.05f) },
                new[] { (0, 0.5f), (3, 0.45f) },
                new[] { (0, 0.6f), (3, 0.5f) }
            };
            //cluster sparse data into two clusters and use cosine distance (Spherical k-Means)
            km.UseSphericalKMeans = true;
            (clustering, centroids) = km.Cluster(sparseData, 2);
            /* OUTPUT:
             * clustering: 0,0,1,1
             * centroids: [[(0, 0.18335475), (3, 0.9806314), (7, 0.0618031), (6, 0.030387914)],
             *             [(0, 0.7558947), (3, 0.6546932)]] */
            Trace.WriteLine($"clustering: {string.Join(',', clustering)}");
            Trace.WriteLine("centroids:");
            foreach (var c in centroids)
                Trace.WriteLine(c);

            var documents = new[]
            {
                "I went shopping for groceries and also bought tea",
                "This hotel is amazing and the view is perfect",
                "My shopping heist resulted in lots of new shoes",
                "The rooms in this hotel are a bit dirty",
                "my three fav things to do: shopping, shopping, shopping"
            };
            //obtain sparse vector representations using ElskeLib
            var elske = KeyphraseExtractor.CreateFromDocuments(documents);
            elske.StopWords = StopWords.EnglishStopWords;
            var docVectors = documents
                .Select(doc => elske.GenerateBoWVector(doc, true));
            //run clustering three times
            km.UseSphericalKMeans = true;
            (clustering, centroids) = km.Cluster(docVectors, 2, 3);
            //output of clustering: 1,0,1,0,1
            Trace.WriteLine($"clustering: {string.Join(',', clustering)}");
            //use centroids to determine most relevant tokens for each cluster
            for (int i = 0; i < centroids.Length; i++)
            {
                var c = centroids[i];
                //we can regard each centroid as a weighted word list
                //get the two entries with the highest weight and retrieve corresponding word
                var words = c.AsEnumerable()
                    .OrderByDescending(p => p.value)
                    .Take(2)
                    .Select(p => elske.ReferenceIdxMap.GetToken(p.key));
                Trace.WriteLine($"cluster {i}: {string.Join(',', words)}");
            }
            /*
             * OUTPUT:
             * cluster 0: hotel,amazing
             * cluster 1: shopping,groceries
             */
        }

        private static string GetClusterDescription(FlexibleVector centroid, KeyphraseExtractor elske)
        {
            if (centroid.Length == 0)
                return "";
            var sb = new StringBuilder();
            centroid.ToArrays(out var indexes, out var values);
            Array.Sort(values, indexes);
            var numVals = Math.Min(5, indexes.Length);
            for (int i = 1; i <= numVals; i++)
            {
                var idx = indexes[^i];
                var val = values[^i];
                var token = elske.ReferenceIdxMap.GetToken(idx);
                if (sb.Length > 0)
                    sb.Append(", ");
                sb.Append($"{token} ({val})");
            }

            if (indexes.Length > 5)
                sb.Append(", ...");

            return sb.ToString();
        }


        private void RunBasicKMeansVariations(TestSet set, int k)
        {
            Trace.WriteLine($"\r\n== {set.Name} | {k} clusters ==\r\n");

            var kmeans = new KMeans();

            foreach (var r in new[] { 1, 10 })
            {
                Trace.WriteLine($"\r\n{r} run(s)\r\n");

                foreach (var useSpherical in new[] { false, true })
                {
                    foreach (var usePP in new[] { false, true })
                    {
                        foreach (var useNCC in new[] { false, true })
                        {
                            foreach (var useINDEX in new[] { false, true })
                            {
                                if(useINDEX && (!useSpherical || !set.Data[0].IsSparse))
                                    continue;

                                kmeans.UseSphericalKMeans = useSpherical;
                                kmeans.UseKMeansPlusPlusInitialization = usePP;
                                kmeans.UseClustersChangedMap = useNCC;
                                kmeans.UseIndexedMeans = useINDEX;
                                var watch = Stopwatch.StartNew();
                                var (clustering, centroids, nmi) = RunClusterTest(set, kmeans, k, r);
                                watch.Stop();
                                Assert.AreEqual(set.Data!.Length, clustering.Length);
                                Assert.AreEqual(k, centroids.Length);
                                Trace.WriteLine($"{KMeansParaString(kmeans)} | {watch.Elapsed} | NMI {nmi}");
                            }
                        }
                    }
                }

            }

        }

        private static string KMeansParaString(KMeans kmeans)
        {
            return $"{(kmeans.UseSphericalKMeans ? "cosine   " : "Euclidean")}, NCC={kmeans.UseClustersChangedMap}, Indexed={kmeans.UseIndexedMeans}, ++init={kmeans.UseKMeansPlusPlusInitialization}";
        }

        private (int[] clustering, FlexibleVector[] centroids, double nmi) RunClusterTest(TestSet set, KMeans kmeans, int k, int numRuns)
        {
            var (clustering, centroids) = kmeans.Cluster(set.Data!, k, numRuns);
            var nmi = 0d;
            if (set.Labels != null)
                (_, nmi) = EvaluationMetrics.CalculateNormalizedMutualInformation(clustering, set.Labels);
            return (clustering, centroids, nmi);
        }

        

    }
}