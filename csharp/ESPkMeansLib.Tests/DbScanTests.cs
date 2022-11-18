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
using System.Runtime.InteropServices.ComTypes;
using System.Text;
using System.Threading.Tasks;
using ElskeLib.Model;
using ElskeLib.Utils;
using ESPkMeansLib.Helpers;
using ESPkMeansLib.Model;
using ESPkMeansLib.Tests.datasets;
using ESPkMeansLib.Tests.Helpers;

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

        [TestMethod]
        public void TwitterClusterTest()
        {
            var (data, elske, origData) = LoadTextData("twitter_hampshire");
            Trace.WriteLine("set loaded.");
            var set = new TestSet
            {
                Data = data
            };
            var dbs = new DbScan
            {
                MaxDistance = 0.5f,
                EnableLogging = true
            };

            var (clustering, clusterCounts, _) = RunClusterTest(set, dbs);
            Trace.WriteLine("");
            PrintClustersInfo(set.Data, clustering, clusterCounts, true, elske.ReferenceIdxMap, origData);
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
            bool useCosine, WordIdxMap? idxMap = null, string[]? origData = null)
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
                var cid = i;
                var centroid = means[i];
                var clusterString = idxMap == null ? "" : KMeansTests.GetClusterDescription(centroid, idxMap);
                Trace.WriteLine($"CLUSTER {i} | {clusterCounts[i]} items | {clusterString} | {centroid}");
                var bestDist = double.MaxValue;
                var bestId = -1;
                var worstDist = double.MinValue;
                var worstId = -1;
                Parallel.For(0, data.Length, j =>
                {
                    if (clustering[j] != cid)
                        return;
                    var vec = data[j];
                    var dist = useCosine
                        ? centroid.CosineDistanceWith(vec)
                        : centroid.SquaredEuclideanDistanceWith(vec);
                    lock (centroid)
                    {
                        if (dist < bestDist)
                        {
                            bestDist = dist;
                            bestId = j;
                        }

                        if (dist > worstDist)
                        {
                            worstDist = dist;
                            worstId = j;
                        }
                    }
                });
                Trace.WriteLine($"\tREP: {data[bestId]}");
                if(origData != null)
                    Trace.WriteLine($"\tREP Text: {origData[bestId]}");
                Trace.WriteLine($"\tW REP: {data[worstId]}");
                if (origData != null)
                    Trace.WriteLine($"\tW REP Text: {origData[worstId]}");
                Trace.WriteLine("");

            }


        }

        internal static (FlexibleVector[] data, KeyphraseExtractor elske, string[] origData) LoadTextData(string name)
        {

            var fn = $"datasets/{name}.csv.gz";
            var origData = FileHelper.ReadLines(fn).ToArray();
            var elske = KeyphraseExtractor.CreateFromDocuments(
                origData, new ElskeCreationSettings
                {
                    BuildReferenceCollection = false,
                    DoNotCountPairs = true,
                    IsDebugStopwatchEnabled = true,
                    TokenizationSettings = new TokenizationSettings
                    {
                        ConvertToLowercase = true,
                        RetainPunctuationCharacters = false,
                        HtmlDecode = true,
                        TwitterRemoveRetweetInfo = true,
                        TwitterRemoveUrls = true,
                        TwitterRemoveUserMentions = true
                    }
                });
            var data = origData
                .Select(l => new FlexibleVector(elske.GenerateBoWVector(l, true)))
                .ToArray();
            return (data, elske, origData);
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