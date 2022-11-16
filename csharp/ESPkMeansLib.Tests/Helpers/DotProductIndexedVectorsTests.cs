/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using ElskeLib;
using ESPkMeansLib.Helpers;
using ESPkMeansLib.Model;
using ESPkMeansLib.Tests.datasets;
using ESPkMeansLib.Tests.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ESPkMeansLib.Tests.Helpers
{
    [TestClass]
    public class DotProductIndexedVectorsTests
    {
        private static (List<FlexibleVector> queryVectors, FlexibleVector[] indexVectors) CreateQueryAndIndexVectors(int nInitQuery = 10, int nIndex = 10_000)
        {
            var queryVectors = FlexibleVectorTests.CreateRandomVectors(nInitQuery, true).ToList();
            var indexVectors = FlexibleVectorTests.CreateRandomVectors(nIndex, true)
                .Where(v => v.Length >= 1).ToList();

            var rnd = new Random(59649269);
            for (int r = 0; r < 10; r++)
            {
                for (int i = 0; i < nInitQuery; i++)
                {
                    var d = queryVectors[i].ToDictionary();
                    var keys = d.Keys.ToArray();
                    foreach (var k in keys)
                    {
                        var mode = rnd.Next(10);
                        switch (mode)
                        {
                            default:
                                break;
                            case 0:
                                d.Remove(k);
                                break;
                            case 1:
                            case 2:
                            case 3:
                                var diff = (float)(rnd.NextDouble() * -.5);
                                d[k] += diff;
                                break;
                        }

                        var toAdd = rnd.Next(9);
                        for (int j = 0; j < toAdd; j++)
                        {
                            int idx;
                            do
                            {
                                idx = rnd.Next(100_000);
                            } while (d.ContainsKey(idx));
                            d.Add(idx, (float)(rnd.NextDouble() * 20 - 10));
                        }
                    }
                    if (d.Count == 0)
                        continue;
                    indexVectors.Add(new FlexibleVector(d));
                }
            }

            foreach (var v in queryVectors)
            {
                v.NormalizeAsUnitVector();
            }
            foreach (var v in indexVectors)
            {
                v.NormalizeAsUnitVector();
            }
            return (queryVectors, indexVectors.ToArray());
        }

        private static (List<FlexibleVector> queryVectors, FlexibleVector[] indexVectors) GetWordVectors(int nInitQuery = 1000)
        {
            var vectors = FlexibleVector.ArrayFromFile("datasets/wordvectors.bin.gz");
            Trace.WriteLine($"{vectors.Length} vectors loaded");
            vectors.Shuffle();
            return (vectors[..nInitQuery].ToList(), vectors[nInitQuery..]);
        }

        [TestMethod]
        public void DotProductIndexedVectorsTest()
        {

            var (queryVectors, indexVectors) = CreateQueryAndIndexVectors();

            var thresholds = new[] { 0.05f, 0.25f, 0.4f, 0.6f };

            var db = new DotProductIndexedVectors();

            var watch = new Stopwatch();

            for (int i = 0; i < 2; i++)
            {
                watch.Restart();
                db.Set(indexVectors);
                Trace.WriteLine($"{watch.Elapsed} for creating db");


                Assert.AreEqual(indexVectors.Length, db.VectorsCount);

                foreach (var qV in queryVectors)
                {
                    foreach (var threshold in thresholds)
                    {
                        var tSet = indexVectors.Select((v, i) => (v, i))
                            .Where(p => p.v.DotProductWith(qV) >= threshold).Select(p => p.i).ToList();

                        var retrieved = db.GetNearbyVectors(qV)
                            .Where(p => p.dotProduct >= threshold).ToList();
                        var intersection = tSet.Intersect(retrieved.Select(p => p.id));
                        Assert.AreEqual(tSet.Count, intersection.Count());
                        Trace.WriteLine($"th {threshold}: {tSet.Count} above th, {retrieved.Count} retrieved");

                        retrieved = db.GetNearbyVectors(qV, threshold)
                            .Where(p => p.dotProduct >= threshold).ToList();
                        intersection = tSet.Intersect(retrieved.Select(p => p.id));
                        Assert.AreEqual(tSet.Count, intersection.Count());
                    }
                }
                db.Clear();
            }

        }

        [TestMethod]
        public void DotProductIndexedVectorsWordVectorsTest()
        {
            var (queryVectors, indexVectors) = GetWordVectors();
            var thresholds = new[] { 0.05f, 0.25f, 0.4f, 0.6f, 0.85f };
            var db = new DotProductIndexedVectors();
            var watch = new Stopwatch();
            db.Set(indexVectors);
            Trace.WriteLine($"{watch.Elapsed} for creating db");
            Assert.AreEqual(indexVectors.Length, db.VectorsCount);

            Parallel.For(0, queryVectors.Count, r =>
            {
                var qV = queryVectors[r];
                foreach (var threshold in thresholds)
                {
                    var tSet = indexVectors.Select((v, i) => (v, i))
                        .Where(p => p.v.DotProductWith(qV) >= threshold).Select(p => p.i).ToList();
                    var retrieved = db.GetNearbyVectors(qV, threshold)
                        .Where(p => p.dotProduct >= threshold - 0.01f).ToList();
                    var intersection = tSet.Intersect(retrieved.Select(p => p.id));
                    Assert.AreEqual(tSet.Count, intersection.Count(),
                        $"not all {tSet.Count} required vectors with dp >= {threshold} retrieved");
                }

            });
        }


        [TestMethod]
        public void GetKNearestNeighborsTest()
        {
            var (queryVectors, indexVectors) = CreateQueryAndIndexVectors();
            var tmpFn = Guid.NewGuid().ToString("N");
            try
            {
                for (int i = 0; i < 2; i++)
                {
                    DotProductIndexedVectors db;
                    if (i == 0)
                    {
                        db = new DotProductIndexedVectors();
                        db.Set(indexVectors);
                        db.ToFile(tmpFn);
                    }
                    else
                    {
                        db = DotProductIndexedVectors.FromFile(tmpFn);
                        Trace.WriteLine("loaded.");
                    }

                    PerformKNearestNeighborsTest(db, queryVectors, indexVectors);
                }
            }
            finally
            {
                if (File.Exists(tmpFn))
                    File.Delete(tmpFn);
            }
        }

        [TestMethod]
        public void GetKNearestNeighborsWordVectorsTest()
        {
            var (queryVectors, indexVectors) = GetWordVectors();
            var db = new DotProductIndexedVectors();
            db.Set(indexVectors);
            PerformKNearestNeighborsTest(db, queryVectors, indexVectors);
        }


        private static void PerformKNearestNeighborsTest(DotProductIndexedVectors db, List<FlexibleVector> queryVectors,
            FlexibleVector[] indexVectors)
        {
            var thresholds = new[] { 0, 1, 3, 5, 8, 11, 20, 100, 10_000 };
            Parallel.For(0, queryVectors.Count, r =>
            {
                var v = queryVectors[r];
                var indexVectorsDotProducts = new float[indexVectors.Length];
                var indexVectorsKeys = Enumerable.Range(0, indexVectors.Length).ToArray();
                for (int i = 0; i < indexVectors.Length; i++)
                {
                    indexVectorsDotProducts[i] = indexVectors[i].DotProductWith(v);
                }
                Array.Sort(indexVectorsDotProducts, indexVectorsKeys);
                Array.Reverse(indexVectorsDotProducts);
                Array.Reverse(indexVectorsKeys);
                foreach (var k in thresholds)
                {
                    if (v.Length == 0 || k == 0)
                    {
                        Assert.AreEqual(0, db.GetKNearestVectors(v, k).Count);
                        continue;
                    }

                    var groundTruthLen = k;
                    var minGroundTruthLen = k;
                    var minDotProductInGroundTruth = indexVectorsDotProducts[k - 1];
                    for (int i = 0; i < indexVectorsDotProducts.Length && i < k; i++)
                    {
                        var dp = indexVectorsDotProducts[i];
                        if (minGroundTruthLen == k && dp <= 0.0001f) //due to rounding errors we might not catch "near" neighbors that still have very low dot product
                            minGroundTruthLen = i;
                        if (dp <= 0)
                        {
                            groundTruthLen = i;
                            minDotProductInGroundTruth = i == 0 ? 0 : indexVectorsDotProducts[i - 1];
                            break;
                        }
                    }
                    if (groundTruthLen == 0)
                    {
                        Assert.AreEqual(0, db.GetKNearestVectors(v, k).Count(p => p.dotProduct > 0));
                        continue;
                    }

                    var groundTruthComplete = new HashSet<int>(indexVectorsKeys[..groundTruthLen]);

                    for (int i = groundTruthLen; i < indexVectorsDotProducts.Length; i++)
                    {
                        var dp = indexVectorsDotProducts[i];
                        if (dp < 0 || dp < minDotProductInGroundTruth - 0.001f)
                            break;
                        groundTruthComplete.Add(indexVectorsKeys[i]);

                    }

                    var res = db.GetKNearestVectors(v, k).ToArray();
                    //Trace.WriteLine($"k {k}: {res.Length} / {groundTruth.Length}");
                    Assert.IsTrue(res.Length >= minGroundTruthLen && res.Length <= groundTruthLen,
                        $"groundTruth len ({minGroundTruthLen} to {groundTruthLen}) and res.Length ({res.Length}) mismatch, min dp {minDotProductInGroundTruth}");

                    Assert.AreEqual(res.Length, res.Count(p => groundTruthComplete.Contains(p.id)),
                        $"not all expected nearest neighbors found, min dp {minDotProductInGroundTruth}");

                    if (k != 1) continue;

                    var (k1res, k1dp) = db.GetNearestVector(v);
                    if (v.Length == 0)
                        Assert.AreEqual(-1, k1res);
                    else
                    {
                        Assert.AreEqual(v.DotProductWith(db.GetVectorById(k1res)), k1dp, 0.0001f,
                            "GetNearestNeighbor did not return right vector");
                        Assert.AreEqual(minDotProductInGroundTruth, k1dp, 0.0001f, "GetNearestNeighbor calculated wrong dot product");
                    }
                }

                //Trace.WriteLine("");
            });
        }

        [TestMethod]
        public void IndexingSpeedTest()
        {
            var set = FlexibleVectorTests.CreateRandomVectors(5_000, true, 2_000);
            Parallel.For(0, set.Length, i => set[i].NormalizeAsUnitVector());
            var db = new DotProductIndexedVectors();
            db.Set(set.Take(100).ToArray()); //warmup
            var watch = Stopwatch.StartNew();
            db = new DotProductIndexedVectors();
            db.Set(set);
            Trace.WriteLine($"{watch.Elapsed} random vecs"); watch.Restart();
            set = FlexibleVector.ArrayFromFile("datasets/wordvectors.bin.gz");
            watch.Restart(); db = new DotProductIndexedVectors();
            db.Set(set);
            Trace.WriteLine($"{watch.Elapsed} word vecs"); watch.Restart();

        }


        [TestMethod]
        public void NearestVectorsSpeedTest()
        {
            const int numClusters = 2000;
            var count = 0;
            var watch = Stopwatch.StartNew();
            var set = TestSet.LoadArxiv100K();
            Trace.WriteLine($"{watch.Elapsed} data loaded");
            watch.Restart();
            var clustering = new int[set.Data!.Length];
            var rnd = new Random();
            for (int i = 0; i < clustering.Length; i++)
                clustering[i] = rnd.Next(numClusters);
            Trace.WriteLine($"{watch.Elapsed} clustering set");
            watch.Restart();
            var kMeans = new KMeans { UseSphericalKMeans = true, UseKMeansPlusPlusInitialization = false };
            //var (means, _) = MeanCalculations.GetMeans(set.Data, numClusters, clustering, true);
            var (_, means) = kMeans.Cluster(set.Data, numClusters);
            Trace.WriteLine($"{watch.Elapsed} means computed");
            watch.Restart();
            var targets = means; // means.Skip(1000).ToArray();
            var db = new DotProductIndexedVectors();
            db.Set(targets);
            Trace.WriteLine($"{watch.Elapsed} means indexed");
            watch.Restart();

            for (int i = 0; i < 100; i++)
            {
                var vec = means[i];
                db.GetNearestVector(vec);
            }
            for (int i = 0; i < 1000; i++)
            {
                var vec = set.Data[i];
                db.GetNearestVector(vec);
            }
            watch.Stop();
            for (int i = 90; i < 100; i++)
            {
                var vec = means[i];
                var (id, dp) = db.GetNearestVector(vec);
                var target = targets.MaxItem(v => v.DotProductWith(vec));
                Assert.AreEqual(Array.IndexOf(targets, target), id);
                Assert.AreEqual(target.DotProductWith(vec), dp, 0.0001f);
            }

            Trace.WriteLine($"{watch.Elapsed} GetNearestVector");
            watch.Restart();
            for (int i = 100; i < 200; i++)
            {
                var vec = means[i];
                db.GetKNearestVectors(vec, 10);
            }

            for (int i = 1000; i < 2000; i++)
            {
                var vec = set.Data[i];
                db.GetKNearestVectors(vec, 10);
            }
            watch.Stop();
            for (int i = 180; i < 200; i++)
            {
                var vec = means[i];
                var list = db.GetKNearestVectors(vec, 10);
                var targetList = targets
                    .OrderByDescending(v => v.DotProductWith(vec))
                    .Take(10).Where(v => v.DotProductWith(vec) > 0).ToArray();
                Assert.AreEqual(targetList.Length, list.Count);
                for (int j = 0; j < targetList.Length; j++)
                {
                    var idx = Array.IndexOf(targets, targetList[j]);
                    Assert.IsTrue(list.Any(it => it.id == idx));
                }
            }

            var el = watch.Elapsed;
            Trace.WriteLine($"{el} Get10NearestVectors");
            watch.Restart();

            for (int i = 200; i < 300; i++)
            {
                var vec = means[i];
                db.GetKNearestVectors(vec, 30);
            }
            watch.Stop();
            
            for (int i = 280; i < 300; i++)
            {
                var vec = means[i];
                var list = db.GetKNearestVectors(vec, 30);
                var targetList = targets
                    .OrderByDescending(v => v.DotProductWith(vec))
                    .Take(30).Where(v => v.DotProductWith(vec) > 0).ToArray();
                Assert.AreEqual(targetList.Length, list.Count);
                for (int j = 0; j < targetList.Length; j++)
                {
                    var idx = Array.IndexOf(targets, targetList[j]);
                    Assert.IsTrue(list.Any(it => it.id == idx));
                }
            }
            Trace.WriteLine($"{watch.Elapsed} Get30NearestVectors");
            watch.Restart();

            for (int i = 300; i < 400; i++)
            {
                var vec = means[i];
                db.GetNearbyVectors(vec);
            }
            watch.Stop();
            for (int i = 390; i < 400; i++)
            {
                var vec = means[i];
                var list = db.GetNearbyVectors(vec);
                var targetList = targets
                    .Where(v => v.DotProductWith(vec) > 0).ToArray();
                Assert.AreEqual(targetList.Length, list.Count);
                for (int j = 0; j < targetList.Length; j++)
                {
                    var idx = Array.IndexOf(targets, targetList[j]);
                    Assert.IsTrue(list.Any(it => it.id == idx));
                }
            }

            Trace.WriteLine($"{watch.Elapsed} GetNearbyVectors");
            watch.Restart();

            count = 0;
            for (int i = 400; i < 500; i++)
            {
                var vec = means[i];
                db.GetNearbyVectors(vec, 0.4f);
            }
            watch.Stop();
            for (int i = 490; i < 500; i++)
            {
                var vec = means[i];
                var list = db.GetNearbyVectors(vec, 0.4f);
                //count += list.Count;
                var targetList = targets
                    .Where(v => v.DotProductWith(vec) >= 0.4f).ToArray();
                count += targetList.Length;
                Assert.IsTrue(targetList.Length <= list.Count);
                for (int j = 0; j < targetList.Length; j++)
                {
                    var idx = Array.IndexOf(targets, targetList[j]);
                    Assert.IsTrue(list.Any(it => it.id == idx));
                }
            }

            Trace.WriteLine($"{watch.Elapsed} GetNearbyVectors 0.4 with {count} results");
            watch.Restart();


            count = 0;
            for (int i = 500; i < 600; i++)
            {
                var vec = means[i];
                db.GetNearbyVectors(vec, 0.3f);
            }
            watch.Stop();
            for (int i = 590; i < 600; i++)
            {
                var vec = means[i];
                var list = db.GetNearbyVectors(vec, 0.3f);
                //count += list.Count;
                var targetList = targets
                    .Where(v => v.DotProductWith(vec) >= 0.3f).ToArray();
                count += targetList.Length;
                Assert.IsTrue(targetList.Length <= list.Count);
                for (int j = 0; j < targetList.Length; j++)
                {
                    var idx = Array.IndexOf(targets, targetList[j]);
                    Assert.IsTrue(list.Any(it => it.id == idx));
                }
            }

            Trace.WriteLine($"{watch.Elapsed} GetNearbyVectors 0.3 with {count} results");
            watch.Restart();


            count = 0;
            for (int i = 600; i < 700; i++)
            {
                var vec = means[i];
                count += db.GetNearbyVectors(vec, 0.6f).Count;
            }
            watch.Stop();

            Trace.WriteLine($"{watch.Elapsed} GetNearbyVectors 0.6 with {count/100d:f1} results");
            watch.Restart();


        }

    }
}