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