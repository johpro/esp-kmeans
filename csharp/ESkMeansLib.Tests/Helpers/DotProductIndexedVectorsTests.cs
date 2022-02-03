using System.Diagnostics;
using System.Linq;
using ESkMeansLib.Helpers;
using ESkMeansLib.Tests.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ESkMeansLib.Tests.Helpers
{
    [TestClass]
    public class DotProductIndexedVectorsTests
    {
        [TestMethod]
        public void DotProductIndexedVectorsTest()
        {
            var queryVectors = FlexibleVectorTests.CreateRandomVectors(10, true);
            var indexVectors = FlexibleVectorTests.CreateRandomVectors(4_000, true)
                .Where(v => v.Length >= 1).ToArray();

            foreach (var v in queryVectors)
            {
                v.NormalizeAsUnitVector();
            }
            foreach (var v in indexVectors)
            {
                v.NormalizeAsUnitVector();
            }

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
        public void DotProductIndexedVectorsTest1()
        {
            Assert.Fail();
        }

        [TestMethod]
        public void ClearTest()
        {
            Assert.Fail();
        }

        [TestMethod]
        public void SetTest()
        {
            Assert.Fail();
        }

        [TestMethod]
        public void AddTest()
        {
            Assert.Fail();
        }

        [TestMethod]
        public void GetNearbyVectorsTest()
        {
            Assert.Fail();
        }

        [TestMethod]
        public void GetNearbyVectorsTest1()
        {
            Assert.Fail();
        }

        [TestMethod]
        public void SquareTest()
        {
            Assert.Fail();
        }
    }
}