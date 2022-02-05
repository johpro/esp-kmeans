using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using ESPkMeansLib.Helpers;
using ESPkMeansLib.Model;
using ESPkMeansLib.Tests.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ESPkMeansLib.Tests.Helpers
{
    [TestClass]
    public class DotProductIndexedVectorsTests
    {
        private static (List<FlexibleVector> queryVectors, FlexibleVector[] indexVectors) CreateQueryAndIndexVectors(int nInitQuery = 10, int nIndex = 1_000)
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
                                var diff = (float)(rnd.NextDouble() *  - .5);
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
        [TestMethod]
        public void DotProductIndexedVectorsTest()
        {

            var (queryVectors, indexVectors) = CreateQueryAndIndexVectors();

            var thresholds = new[] { 0.05f, 0.25f, 0.4f, 0.6f };

            var db = new DotProductIndexedVectors();


            for (int i = 0; i < 2; i++)
            {
                db.Set(indexVectors);
                foreach (var qV in queryVectors)
                {
                    foreach (var threshold in thresholds)
                    {
                        var tSet = indexVectors.Select((v, i) => (v, i))
                            .Where(p => p.v.DotProductWith(qV) >= threshold).Select(p => p.i).ToList();

                        var retrieved = db.GetNearbyVectors(qV, threshold).ToList();
                        var intersection = tSet.Intersect(retrieved);
                        Assert.AreEqual(tSet.Count, intersection.Count());
                        Trace.WriteLine($"th {threshold}: {tSet.Count} above th, {retrieved.Count} retrieved");
                    }
                }
                db.Clear();
            }

            db = new DotProductIndexedVectors(new[] { 0f });
            db.Set(indexVectors);
            foreach (var qV in queryVectors)
            {
                var tSet = indexVectors.Select((v, i) => (v, i))
                    .Where(p => p.v.DotProductWith(qV) > 0).Select(p => p.i).ToList();

                var retrieved = db.GetNearbyVectors(qV).ToList();
                var intersection = tSet.Intersect(retrieved);
                Assert.AreEqual(tSet.Count, intersection.Count());
                Trace.WriteLine($"th 0: {tSet.Count} above th, {retrieved.Count} retrieved");

            }

        }
        

        [TestMethod]
        public void GetKNearestNeighborsTest()
        {
            var (queryVectors, indexVectors) = CreateQueryAndIndexVectors();
            var db = new DotProductIndexedVectors();
            db.Set(indexVectors);
            var thresholds = new[] { 0, 1, 3, 5, 8, 11, 20, 100, 10_000 };
            foreach (var v in queryVectors)
            {
                foreach (var k in thresholds)
                {
                    if (v.Length == 0 || k == 0)
                    {
                        Assert.AreEqual(0, db.GetKNearestVectors(v, k).Length);
                        continue;
                    }

                    var groundTruth = indexVectors.OrderByDescending(v2 => v.DotProductWith(v2))
                        .Take(k)
                        .Where(v2 => v.DotProductWith(v2) > 0)
                        .ToArray();

                    var res = db.GetKNearestVectors(v, k);
                    Trace.WriteLine($"k {k}: {res.Length} / {groundTruth.Length}");
                    Assert.AreEqual(groundTruth.Length, res.Length);
                    Assert.AreEqual(res.Length, res.Select(id => db.GetVectorById(id)).Intersect(groundTruth).Count());
                }
                Trace.WriteLine("");
            }
        }
        

        [TestMethod]
        public unsafe void SquareTest()
        {
            var vecs = FlexibleVectorTests.CreateRandomVectors(100, false)
                .Select(v => v.Values.ToArray()).ToArray();

            foreach (var vec in vecs)
            {
                var res = new float[vec.Length];
                fixed(float* v = vec, v2 = res)
                    DotProductIndexedVectors.Square(res.Length, v, v2);
                for (int i = 0; i < res.Length; i++)
                {
                    Assert.AreEqual(vec[i]*vec[i], res[i], 0.0001f);
                }
            }
            

        }
    }
}