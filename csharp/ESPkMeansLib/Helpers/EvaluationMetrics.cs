/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

using System.Collections.Concurrent;
using ESPkMeansLib.Model;

namespace ESPkMeansLib.Helpers
{
    public static class EvaluationMetrics
    {
        /// <summary>
        /// Calculate sum of data point's distance to their centroid
        /// </summary>
        /// <param name="data"></param>
        /// <param name="clustering"></param>
        /// <param name="means"></param>
        /// <param name="useCosine">whether to use the cosine distance (true) or Euclidean distance (false)</param>
        /// <returns></returns>
        public static double CalculateDistortion(FlexibleVector[] data, int[] clustering, FlexibleVector[] means, bool useCosine)
        {
            var distancesToClusterSum = 0d;
            var lck = new object();
            var partition = Partitioner.Create(0, data.Length);

            Parallel.ForEach(partition, range =>
            {
                var sum = 0d;
                for (int i = range.Item1; i < range.Item2; i++)
                {
                    var dataRow = data[i];
                    var curCluster = clustering[i];
                    var mean = means[curCluster];
                    var distanceToCluster = useCosine
                        ? dataRow.CosineDistanceWith(mean)
                        : Math.Sqrt(dataRow.SquaredEuclideanDistanceWith(mean));
                    sum += distanceToCluster;
                }

                lock (lck)
                {
                    distancesToClusterSum += sum;
                }
            });

            Thread.MemoryBarrier();
            return distancesToClusterSum;
        }


        public static double CalculateMutualInformation(IEnumerable<(int a, int b)> pairs)
        {
            var pairCounts = new Dictionary<(int, int), int>();
            var aCounts = new Dictionary<int, int>();
            var bCounts = new Dictionary<int, int>();

            var n = 0;
            foreach (var p in pairs)
            {
                pairCounts.IncrementItem(p);
                aCounts.IncrementItem(p.a);
                bCounts.IncrementItem(p.b);
                n++;
            }

            var pairSum = pairCounts.Sum(c => c.Value * Math.Log2(c.Value));
            var aSum = aCounts.Sum(c => c.Value * Math.Log2(c.Value));
            var bSum = bCounts.Sum(c => c.Value * Math.Log2(c.Value));

            return Math.Log2(n) + (1d / n) * (pairSum - aSum - bSum);
        }


        public static double CalculateEntropy(IEnumerable<int> values)
        {
            var counts = new Dictionary<int, int>();
            var totalCount = 0;
            foreach (var value in values)
            {
                counts.IncrementItem(value);
                totalCount++;
            }

            if (totalCount == 0)
                return 0;

            var totalLog = Math.Log2(totalCount);
            var sum = 0d;
            foreach (var p in counts)
            {
                var prob = p.Value / (double)totalCount;
                sum += prob * (Math.Log2(p.Value) - totalLog);
            }

            return -sum;
        }

        public static double CalculateMutualInformation(IEnumerable<int> clustering1, IEnumerable<int> clustering2)
        {
            return CalculateMutualInformation(clustering1.Zip(clustering2));
        }

        public static (double mi, double nmi) CalculateNormalizedMutualInformation(
            IEnumerable<int> clustering1, IEnumerable<int> clustering2)
        {
            var c1 = clustering1 as IList<int> ?? clustering1.ToList();
            var c2 = clustering2 as IList<int> ?? clustering2.ToList();
            var mi = CalculateMutualInformation(c1, c2);
            if (mi <= double.Epsilon)
                return (0, 0);

            var h1 = CalculateEntropy(c1);
            var h2 = CalculateEntropy(c2);
            var denom = (h1 + h2) / 2;
            if (denom <= double.Epsilon)
                return (mi, 0);

            return (mi, mi / denom);
        }
    }
}
