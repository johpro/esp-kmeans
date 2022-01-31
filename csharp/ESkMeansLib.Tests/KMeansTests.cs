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
using ESkMeansLib.Helpers;
using ESkMeansLib.Model;
using ESkMeansLib.Tests.datasets;

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

        [TestMethod()]
        public void EnsureUnitVectorsTest()
        {
            Assert.Fail();
        }

        [TestMethod()]
        public void GetClusteringTest()
        {
            Assert.Fail();
        }

    }
}