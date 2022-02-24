/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ESPkMeansLib.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ESPkMeansLib.Tests.Helpers
{
    [TestClass]
    public class DictListTests
    {
        [TestMethod]
        public void DictListTest()
        {
            var testData1 = new (int key, int[] list)[]
            {
                (0, new[] { 1, 5, 200 }),
                (40, new[] { 3 }),
                (20, new[] { 4,4 }),
                (30, new[] { 434,1444,3,34,414, 924, 34, 4034, 1323, 42134 }),
            };
            var testData2 = new (int key, int[] list)[]
            {
                (0, new[] { 3 }),
                (40, new[] { 7, 10 }),
                (20, new[] { 4,5,10 }),
                (15, new[] { 34,1444,3,34,414 }),
            };
            var dict = new DictList<int, int>();
            Assert.AreEqual(0, dict.Count);
            Assert.IsFalse(dict.TryGetValue(0, out _));

            foreach ((int key, int[] list) in testData1)
            {
                foreach (var i in list)
                {
                    dict.AddToList(key, i);
                }
            }
            Assert.AreEqual(testData1.Length, dict.Count);
            Assert.AreEqual(3, dict.EntriesCount);
            foreach ((int key, int[] list) in testData1)
            {
                Assert.IsTrue(dict.TryGetValue(key, out var l));
                Assert.AreEqual(list.Length, l.Count);
                Assert.IsTrue(list.SequenceEqual(l));
            }

            dict.Clear();

            Assert.IsFalse(dict.TryGetValue(0, out var l0));
            Assert.IsTrue(l0 == null || l0.Count == 0);

            foreach ((int key, int[] list) in testData2)
            {
                foreach (var i in list)
                {
                    dict.AddToList(key, i);
                }
            }
            Assert.AreEqual(testData2.Length, dict.Count);
            foreach ((int key, int[] list) in testData2)
            {
                Assert.IsTrue(dict.TryGetValue(key, out var l));
                Assert.AreEqual(list.Length, l.Count);
                Assert.IsTrue(list.SequenceEqual(l));
            }



        }
    }
}
