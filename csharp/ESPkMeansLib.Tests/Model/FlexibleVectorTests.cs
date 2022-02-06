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
using System.Text;
using System.Text.Json;
using ESPkMeansLib.Helpers;
using ESPkMeansLib.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ESPkMeansLib.Tests.Model
{
    [TestClass()]
    public class FlexibleVectorTests
    {
        [TestMethod()]
        public void FlexibleVectorCreateAndGetTest()
        {
            try
            {
                Trace.WriteLine(new FlexibleVector(new Dictionary<int, float> { { 2, 3f }, { 5, 4f }, { 2, 8f } }));
                Assert.Fail("it should not be possible to create a sparse vector with non-unique indexes");
            }
            catch (Exception)
            {
                // the creation should fail
            }


            var empty1 = new FlexibleVector(new float[] { });
            var empty2 = new FlexibleVector(new double[] { });
            var empty3 = new FlexibleVector(Enumerable.Empty<(int idx, float val)>());
            var empty4 = new FlexibleVector(new Dictionary<int, float>());

            Assert.AreEqual(0, empty1.Length);
            Assert.AreEqual(0, empty2.Length);
            Assert.AreEqual(0, empty3.Length);
            Assert.AreEqual(0, empty4.Length);

            Assert.AreEqual(0, empty1[0]);
            Assert.AreEqual(0, empty2[0]);
            Assert.AreEqual(0, empty3[0]);
            Assert.AreEqual(0, empty4[0]);

            Assert.AreEqual(0, empty1[10]);
            Assert.AreEqual(0, empty2[10]);
            Assert.AreEqual(0, empty3[10]);
            Assert.AreEqual(0, empty4[10]);

            Assert.IsFalse(empty1.TrySetValue(0, 1f));
            Assert.IsFalse(empty2.TrySetValue(0, 1f));
            Assert.IsFalse(empty3.TrySetValue(0, 1f));
            Assert.IsFalse(empty4.TrySetValue(0, 1f));

            Assert.AreEqual(0, empty1.AsEnumerable().Count());
            Assert.AreEqual(0, empty2.AsEnumerable().Count());
            Assert.AreEqual(0, empty3.AsEnumerable().Count());
            Assert.AreEqual(0, empty4.AsEnumerable().Count());

            Assert.IsTrue(empty1.ValueEquals(empty1));
            Assert.IsTrue(empty2.ValueEquals(empty2));
            Assert.IsTrue(empty3.ValueEquals(empty3));
            Assert.IsTrue(empty4.ValueEquals(empty4));
            Assert.IsTrue(empty1.ValueEquals(empty4));
            Assert.IsTrue(empty4.ValueEquals(empty1));



            var one1 = new FlexibleVector(new float[] { 1f });
            var one2 = new FlexibleVector(new double[] { 1d });
            var one3 = new FlexibleVector(new[] { (0, 1f) });
            var one4 = new FlexibleVector(new Dictionary<int, float> { { 0, 1f } });

            Assert.AreEqual(1, one1.Length);
            Assert.AreEqual(1, one2.Length);
            Assert.AreEqual(1, one3.Length);
            Assert.AreEqual(1, one4.Length);


            Assert.AreEqual(1, one1.AsEnumerable().Count());
            Assert.AreEqual(1, one2.AsEnumerable().Count());
            Assert.AreEqual(1, one3.AsEnumerable().Count());
            Assert.AreEqual(1, one4.AsEnumerable().Count());

            Assert.AreEqual(0, one1.AsEnumerable().First().key);
            Assert.AreEqual(1f, one1.AsEnumerable().First().value, float.Epsilon);
            Assert.AreEqual(0, one2.AsEnumerable().First().key);
            Assert.AreEqual(1f, one2.AsEnumerable().First().value, float.Epsilon);
            Assert.AreEqual(0, one3.AsEnumerable().First().key);
            Assert.AreEqual(1f, one3.AsEnumerable().First().value, float.Epsilon);
            Assert.AreEqual(0, one4.AsEnumerable().First().key);
            Assert.AreEqual(1f, one4.AsEnumerable().First().value, float.Epsilon);


            Assert.AreEqual(1f, one1[0], float.Epsilon);
            Assert.AreEqual(1f, one2[0], float.Epsilon);
            Assert.AreEqual(1f, one3[0], float.Epsilon);
            Assert.AreEqual(1f, one4[0], float.Epsilon);

            Assert.AreEqual(0, one1[1]);
            Assert.AreEqual(0, one2[1]);
            Assert.AreEqual(0, one3[1]);
            Assert.AreEqual(0, one4[1]);


            Assert.IsTrue(one1.TrySetValue(0, -1f));
            Assert.IsTrue(one2.TrySetValue(0, -1f));
            Assert.IsTrue(one3.TrySetValue(0, -1f));
            Assert.IsTrue(one4.TrySetValue(0, -1f));

            Assert.IsFalse(one1.TrySetValue(1, 1f));
            Assert.IsFalse(one2.TrySetValue(1, 1f));
            Assert.IsFalse(one3.TrySetValue(1, 1f));
            Assert.IsFalse(one4.TrySetValue(1, 1f));


            Assert.AreEqual(-1f, one1[0], float.Epsilon);
            Assert.AreEqual(-1f, one2[0], float.Epsilon);
            Assert.AreEqual(-1f, one3[0], float.Epsilon);
            Assert.AreEqual(-1f, one4[0], float.Epsilon);


            Assert.IsTrue(one1.ValueEquals(one1));
            Assert.IsTrue(one2.ValueEquals(one2));
            Assert.IsTrue(one3.ValueEquals(one3));
            Assert.IsTrue(one4.ValueEquals(one4));
            Assert.IsTrue(one1.ValueEquals(one4));
            Assert.IsTrue(one4.ValueEquals(one1));



            var arr = new[] { 0f, 1f, 0f, 0f, -5f };
            var indexes = Enumerable.Range(0, arr.Length).ToArray();
            var tuples = indexes.Zip(arr);
            var dict = tuples.ToDictionary(k => k.First, v => v.Second);

            var v1 = new FlexibleVector(arr);
            var v2 = new FlexibleVector(tuples);
            var v3 = new FlexibleVector(dict);

            Assert.AreEqual(arr.Length, v1.Length);
            Assert.AreEqual(arr.Length, v2.Length);
            Assert.AreEqual(arr.Length, v3.Length);

            Assert.IsFalse(v1.IsSparse);
            Assert.IsTrue(v2.IsSparse);
            Assert.IsTrue(v3.IsSparse);


            for (int i = 0; i < arr.Length; i++)
            {
                Assert.AreEqual(arr[i], v1[i], float.Epsilon);
                Assert.AreEqual(arr[i], v2[i], float.Epsilon);
                Assert.AreEqual(arr[i], v3[i], float.Epsilon);
            }

            Assert.AreEqual(0, v1[arr.Length]);
            Assert.AreEqual(0, v2[arr.Length]);
            Assert.AreEqual(0, v3[arr.Length]);

            v1 = v1.ToSparse();
            v2 = v2.ToSparse();
            v3 = v3.ToSparse();
            Assert.AreEqual(2, v1.Length);
            Assert.AreEqual(arr.Length, v2.Length);
            Assert.AreEqual(arr.Length, v3.Length);

            for (int i = 0; i < arr.Length; i++)
            {
                Assert.AreEqual(arr[i], v1[i], float.Epsilon);
                Assert.AreEqual(arr[i], v2[i], float.Epsilon);
                Assert.AreEqual(arr[i], v3[i], float.Epsilon);
            }

            Assert.IsTrue(v1.ValueEquals(v2));
            Assert.IsTrue(v1.ValueEquals(v3));
            Assert.IsTrue(v2.ValueEquals(v1));
            Assert.IsTrue(v3.ValueEquals(v1));

            v1 = v1.ToDense(arr.Length);
            Assert.IsTrue(v1.ValueEquals(v1));
            Assert.IsTrue(v2.ValueEquals(v2));
            Assert.IsTrue(v3.ValueEquals(v3));
            Assert.IsTrue(v1.ValueEquals(v2));
            Assert.IsTrue(v1.ValueEquals(v3));
            Assert.IsTrue(v2.ValueEquals(v1));
            Assert.IsTrue(v3.ValueEquals(v1));

            var v5 = FlexibleVector.CreateSparse(arr);
            Assert.AreEqual(2, v5.Length);
            Assert.AreEqual(2, v5.AsEnumerable().Count());
            for (int i = 0; i < arr.Length; i++)
            {
                Assert.AreEqual(arr[i], v5[i], float.Epsilon);
            }
        }




        [TestMethod()]
        public void CopyValuesToTest()
        {
            var v1 = new FlexibleVector(new[] { (2, 5f), (5, 10f) });
            var v2 = new FlexibleVector(new[] { (2, 2f), (5, 1f) });
            Assert.AreNotEqual(v1.GetSquaredSum(), v2.GetSquaredSum());
            v1.CopyValuesTo(v2);
            Assert.AreEqual(v1.GetSquaredSum(), v2.GetSquaredSum());
            Assert.AreEqual(v1[2], v2[2], float.Epsilon);
            Assert.AreEqual(v1[5], v2[5], float.Epsilon);

        }



        [TestMethod()]
        public void GetMaxVectorLengthTest()
        {
            Assert.AreEqual(5, new FlexibleVector(new float[5]).GetMaxVectorLength());
            Assert.AreEqual(5, new FlexibleVector(new[] { (2, 5f), (4, 10f) }).GetMaxVectorLength());
            var empty1 = new FlexibleVector(new float[] { });
            var empty2 = new FlexibleVector(new double[] { });
            var empty3 = new FlexibleVector(Enumerable.Empty<(int idx, float val)>());
            Assert.AreEqual(0, empty1.GetMaxVectorLength());
            Assert.AreEqual(0, empty2.GetMaxVectorLength());
            Assert.AreEqual(0, empty3.GetMaxVectorLength());
        }

        internal static FlexibleVector[] CreateRandomVectors(int num, bool sparse, int fixedLen = -1)
        {
            var rnd = new Random(345897038);
            var res = new FlexibleVector[num];
            var hs = new HashSet<int>();
            for (int i = 0; i < num; i++)
            {
                var len = fixedLen > 0 ? fixedLen : (i <= 1 ? i : rnd.Next(2, 1025));
                var vals = new float[len];
                for (int j = 0; j < vals.Length; j++)
                {
                    vals[j] = (float)(rnd.NextDouble() * 20 - 10);
                }

                if (!sparse)
                {
                    res[i] = new FlexibleVector(vals);
                    continue;
                }

                hs.Clear();
                var indexes = new int[vals.Length];
                for (int j = 0; j < indexes.Length; j++)
                {
                    int idx;
                    do
                    {
                        idx = rnd.Next(100_000);
                    } while (!hs.Add(idx));

                    indexes[j] = idx;
                }

                res[i] = new FlexibleVector(indexes.Zip(vals));
            }

            return res;
        }

        [TestMethod()]
        public void MultiplyWithTest()
        {
            const float factor = 9;
            var setS = CreateRandomVectors(100, true);
            var setD = CreateRandomVectors(100, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    var sqSum = v.GetSquaredSum();
                    var values = v.Values.ToArray();
                    v.MultiplyWith(factor);
                    Assert.AreEqual(sqSum * factor * factor, v.GetSquaredSum(), 1);
                    for (int i = 0; i < v.Length; i++)
                    {
                        var actual = v.IsSparse ? v[v.Indexes[i]] : v[i];
                        Assert.AreEqual(factor * values[i], actual, 0.00001);
                    }
                }
            }

        }

        [TestMethod()]
        public void DivideByFloatTest()
        {
            const float factor = 3.5f;
            var setS = CreateRandomVectors(100, true);
            var setD = CreateRandomVectors(100, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    var sqSum = v.GetSquaredSum();
                    var values = v.Values.ToArray();
                    v.DivideBy(factor);
                    Assert.AreEqual(sqSum / (factor * factor), v.GetSquaredSum(), 1);
                    for (int i = 0; i < v.Length; i++)
                    {
                        var actual = v.IsSparse ? v[v.Indexes[i]] : v[i];
                        Assert.AreEqual(values[i] / factor, actual, 0.00001);
                    }
                }
            }
        }

        [TestMethod()]
        public void DivideByIntTest()
        {
            const int factor = 5;
            var setS = CreateRandomVectors(100, true);
            var setD = CreateRandomVectors(100, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    var sqSum = v.GetSquaredSum();
                    var values = v.Values.ToArray();
                    v.DivideBy(factor);
                    Assert.AreEqual(sqSum / (factor * factor), v.GetSquaredSum(), 1);
                    for (int i = 0; i < v.Length; i++)
                    {
                        var actual = v.IsSparse ? v[v.Indexes[i]] : v[i];
                        Assert.AreEqual(values[i] / factor, actual, 0.00001);
                    }
                }
            }
        }


        [TestMethod()]
        public void GetSquaredSumTest()
        {
            var setS = CreateRandomVectors(100, true);
            var setD = CreateRandomVectors(100, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    var expected = v.Values.Length == 0 ? 0 : v.Values.ToArray().Sum(it => it * it);
                    Assert.AreEqual(expected, v.GetSquaredSum(), 0.1f);
                    Assert.AreEqual(expected, v.GetSquaredSum(), 0.1f);
                }
            }
        }


        private static float SimpleSquaredDistance(FlexibleVector v1, FlexibleVector v2)
        {
            if (v1.IsSparse)
            {
                var len = Math.Max(v1.GetMaxVectorLength(), v2.GetMaxVectorLength());
                v1 = v1.ToDense(len);
                v2 = v2.ToDense(len);
            }

            var sum = 0d;
            for (int i = 0; i < v1.Values.Length; i++)
            {
                var diff = v1.Values[i] - v2.Values[i];
                sum += diff * diff;
            }

            return (float)sum;
        }

        private static float SimpleDotProduct(FlexibleVector v1, FlexibleVector v2)
        {
            if (v1.IsSparse)
            {
                var len = Math.Max(v1.GetMaxVectorLength(), v2.GetMaxVectorLength());
                v1 = v1.ToDense(len);
                v2 = v2.ToDense(len);
            }

            var sum = 0d;
            for (int i = 0; i < v1.Values.Length; i++)
            {
                sum += v1.Values[i] * v2.Values[i];
            }

            return (float)sum;
        }



        [TestMethod()]
        public void SquaredEuclideanDistanceWithTest()
        {
            var sets = new List<FlexibleVector[]>
            {
                CreateRandomVectors(10, true),
                CreateRandomVectors(1, false, 0),
                CreateRandomVectors(3, false, 1),
                CreateRandomVectors(3, false, 10),
                CreateRandomVectors(3, false, 1000)
            };

            foreach (var set in sets)
            {
                for (var i = 0; i < set.Length; i++)
                {
                    var v1 = set[i];
                    for (int j = 0; j < set.Length; j++)
                    {
                        var v2 = set[j];
                        var distance = v1.SquaredEuclideanDistanceWith(v2);
                        if (i == j)
                            Assert.AreEqual(0, distance, float.Epsilon);
                        else
                            Assert.AreEqual(SimpleSquaredDistance(v1, v2), distance, 0.1f);
                    }
                }
            }

        }
        
        [TestMethod()]
        public void DotProductWithTest()
        {
            var sets = new List<FlexibleVector[]>
            {
                CreateRandomVectors(1, false, 0),
                CreateRandomVectors(5, false, 1),
                CreateRandomVectors(5, false, 10),
                CreateRandomVectors(5, false, 1000),
                CreateRandomVectors(20, true)
            };

            var watch = new Stopwatch();
            foreach (var set in sets)
            {
                watch.Reset();
                for (var i = 0; i < set.Length; i++)
                {
                    var v1 = set[i];
                    for (int j = 0; j < set.Length; j++)
                    {
                        var v2 = set[j];
                        watch.Start();
                        var product = v1.DotProductWith(v2);
                        watch.Stop();
                        Assert.AreEqual(SimpleDotProduct(v1, v2), product, 0.1f);
                        if(!v1.IsSparse)
                            continue;
                        var dict = v2.AsEnumerable().ToDictionary(k => k.key, v => v.value);
                        product = v1.DotProductWith(dict);
                        Assert.AreEqual(SimpleDotProduct(v1, v2), product, 0.1f);
                    }
                }
                Trace.WriteLine(watch.Elapsed);
            }
        }
        


        [TestMethod()]
        public void GetMaxValueTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    var expected = v.Length == 0 ? float.MinValue : v.Values.ToArray().Max();
                    Assert.AreEqual(expected, v.GetMaxValue());
                }
            }
        }


        [TestMethod()]
        public void NormalizeAsUnitVectorTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    if(v.Length == 0)
                        continue;
                    var vPrev = v.Clone();
                    v.NormalizeAsUnitVector();
                    Assert.AreEqual(1, v.GetSquaredSum(), 0.000001f);
                    Assert.AreEqual(vPrev.Values.Length, v.Values.Length);

                }
            }
        }

        [TestMethod()]
        public void ToUnitVectorTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    if (v.Length == 0)
                        continue;

                    var prevSum = v.GetSquaredSum();
                    var v2 = v.ToUnitVector();
                    Assert.AreEqual(1, v2.GetSquaredSum(), 0.000001f);
                    Assert.AreEqual(prevSum, v.GetSquaredSum(), 0.000001f);

                }
            }
        }
        

        [TestMethod()]
        public void ValueEqualsTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    Assert.IsTrue(v.ValueEquals(v));
                    var v2 = v.Clone();
                    Assert.IsTrue(v.ValueEquals(v2));
                    Assert.IsTrue(v2.ValueEquals(v));
                    var vD = v2.Clone().ToDense(v2.GetMaxVectorLength());
                    Assert.IsTrue(v2.ValueEquals(vD));
                    Assert.IsTrue(vD.ValueEquals(v2));
                    if(v.Length == 0)
                        continue;
                    
                    var k = v.AsEnumerable().First().key;
                    Assert.IsTrue(v2.TrySetValue(k, v[k]+0.01f));
                    Assert.IsFalse(v.ValueEquals(v2));
                    Assert.IsFalse(v2.ValueEquals(v));
                    Assert.IsFalse(v2.ValueEquals(vD));
                    Assert.IsFalse(vD.ValueEquals(v2));

                    v2 = v.Clone();
                    k = v.AsEnumerable().Last().key;
                    Assert.IsTrue(v2.TrySetValue(k, v[k] - 0.01f));
                    Assert.IsFalse(v.ValueEquals(v2));
                    Assert.IsFalse(v2.ValueEquals(v));
                    Assert.IsFalse(v2.ValueEquals(vD));
                    Assert.IsFalse(vD.ValueEquals(v2));

                    v2 = v2.ToDense(v2.GetMaxVectorLength());
                    Assert.IsFalse(v.ValueEquals(v2));
                    Assert.IsFalse(v2.ValueEquals(v));
                    Assert.IsFalse(v2.ValueEquals(vD));
                    Assert.IsFalse(vD.ValueEquals(v2));
                }
            }
        }
        

        [TestMethod()]
        public void ToDictionaryTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    var dict = v.ToDictionary();
                    var v2 = new FlexibleVector(dict);
                    Assert.IsTrue(v.ValueEquals(v2));
                }
            }
        }

        [TestMethod()]
        public void CloneTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    var v2 = v.Clone();
                    Assert.AreNotEqual(v, v2);
                    Assert.IsTrue(v.ValueEquals(v2));
                    if(v2.Length == 0)
                        continue;
                    v2.TrySetValue(v2.AsEnumerable().First().key, v2[v2.AsEnumerable().First().key] + 1);
                    Assert.IsFalse(v.ValueEquals(v2));

                }
            }
        }

        [TestMethod()]
        public void ToWriterTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);


            const string tempFn = "lmasfdk482352.bin";
            const string tempFnGz = tempFn + ".gz";

            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    using (var ms = new MemoryStream())
                    {
                        using (var writer = new BinaryWriter(ms, Encoding.Default, true))
                        {
                            v.ToWriter(writer);
                        }

                        ms.Position = 0;

                        using (var reader = new BinaryReader(ms))
                        {
                            var v2 = FlexibleVector.FromReader(reader);
                            Assert.IsTrue(v.ValueEquals(v2));
                        }
                    }

                    try
                    {
                        v.ToFile(tempFn);
                        var v2 = FlexibleVector.FromFile(tempFn);
                        Assert.IsTrue(v.ValueEquals(v2));

                        v.ToFile(tempFnGz);
                        v2 = FlexibleVector.FromFile(tempFnGz);
                        Assert.IsTrue(v.ValueEquals(v2));
                    }
                    finally
                    {
                        if(File.Exists(tempFn))
                            File.Delete(tempFn);
                        if (File.Exists(tempFnGz))
                            File.Delete(tempFnGz);
                    }
                }

                try
                {
                    FlexibleVector.ToFile(set, tempFn);
                    var set2 = FlexibleVector.ArrayFromFile(tempFn);
                    Assert.AreEqual(set.Length, set2.Length);
                    for (int i = 0; i < set.Length; i++)
                    {
                        Assert.IsTrue(set[i].ValueEquals(set2[i]));
                    }

                    FlexibleVector.ToFile(set, tempFnGz);
                    set2 = FlexibleVector.ArrayFromFile(tempFnGz);
                    Assert.AreEqual(set.Length, set2.Length);
                    for (int i = 0; i < set.Length; i++)
                    {
                        Assert.IsTrue(set[i].ValueEquals(set2[i]));
                    }
                }
                finally
                {
                    if (File.Exists(tempFn))
                        File.Delete(tempFn);
                    if (File.Exists(tempFnGz))
                        File.Delete(tempFnGz);
                }

            }

            
        }

        [TestMethod]
        public void ToStringTest()
        {
            var setS = CreateRandomVectors(10, true);
            var setD = CreateRandomVectors(10, false);
            foreach (var set in new[] { setS, setD })
            {
                foreach (var v in set)
                {
                    Trace.WriteLine(v);
                }
            }
        }

        [TestMethod]
        public void ToJsonTest()
        {
            var setS = CreateRandomVectors(3, true);
            var setD = CreateRandomVectors(3, false);
            foreach (var v in setD.Concat(setS))
            {
                var json = v.ToJson();
                Trace.WriteLine(json);
                var v2 = FlexibleVector.FromJson(json);
                Assert.IsTrue(v.ValueEquals(v2));
                using (var ms = new MemoryStream())
                {
                    using (var writer = new Utf8JsonWriter(ms))
                        v.ToJsonWriter(writer);
                    var bytes = ms.ToArray();
                    json = Encoding.UTF8.GetString(bytes);

                }
                Trace.WriteLine(json);
                v2 = FlexibleVector.FromJson(json);
                Assert.IsTrue(v.ValueEquals(v2));
            }
        }
        
    }
}