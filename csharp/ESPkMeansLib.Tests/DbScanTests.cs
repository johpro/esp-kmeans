/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ESPkMeansLib;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ElskeLib.Model;
using ESPkMeansLib.Helpers;
using ESPkMeansLib.Model;
using ESPkMeansLib.Tests.datasets;

namespace ESPkMeansLib.Tests
{
    [TestClass()]
    public class DbScanTests
    {
        [TestMethod()]
        public void NewsgroupsClusterTest()
        {
            var set = TestSet.Load20Newsgroups();
            var dbs = new DbScan
            {
                MaxDistance = 0.6f,
                EnableLogging = true
            };

            var (clustering, clusterCounts, nmi) = RunClusterTest(set, dbs);
            Trace.WriteLine("");
            PrintClustersInfo(set.Data, clustering, clusterCounts, true);
        }

        [TestMethod()]
        public void IrisClusterTest()
        {
            var set = TestSet.LoadIris();
            var dbs = new DbScan
            {
                MaxDistance = 0.4f,
                DistanceMethod = DistanceMethod.Euclidean,
                EnableLogging = true
            };

            var (clustering, clusterCounts, nmi) = RunClusterTest(set, dbs);
            Trace.WriteLine("");
            PrintClustersInfo(set.Data, clustering, clusterCounts, false);
        }

        private static void PrintClustersInfo(FlexibleVector[] data, int[] clustering, int[] clusterCounts,
            bool useCosine, WordIdxMap? idxMap = null)
        {
            var noiseCluster = clusterCounts.Length;
            clustering = clustering.ToArray();
            for (int i = 0; i < clustering.Length; i++)
            {
                if (clustering[i] == -1)
                    clustering[i] = noiseCluster;
            }

            FlexibleVector[] means;
            (means, clusterCounts) = MeanCalculations.GetMeans(data, clusterCounts.Length + 1, clustering, useCosine);
            for (int i = 0; i < clusterCounts.Length; i++)
            {
                var centroid = means[i];
                var clusterString = idxMap == null ? "" : KMeansTests.GetClusterDescription(centroid, idxMap);
                Trace.WriteLine($"CLUSTER {i} | {clusterCounts[i]} items | {clusterString} | {centroid}");
            }


        }

        private static string DbScanParaString(DbScan dbs)
        {
            return $"{dbs.DistanceMethod}, distance = {dbs.MaxDistance}, num samples = {dbs.MinNumSamples}";
        }

        private (int[] clustering, int[] clusterCounts, double nmi) RunClusterTest(TestSet set, DbScan dbs)
        {
            var watch = Stopwatch.StartNew();
            var (clustering, clusterCounts) = dbs.Cluster(set.Data!);
            watch.Stop();
            var nmi = 0d;
            if (set.Labels != null)
                (_, nmi) = EvaluationMetrics.CalculateNormalizedMutualInformation(clustering, set.Labels);
            var numInCluster = clusterCounts.Sum();
            var numNoise = set.Data.Length - numInCluster;
            Trace.WriteLine($"dbscan run in {watch.Elapsed} | {DbScanParaString(dbs)} |" +
                            $" NMI {nmi} | {clusterCounts.Length} clusters found |" +
                            $"{numInCluster} assigned to clusters, {numNoise} noise");
            return (clustering, clusterCounts, nmi);
        }
    }
}