/*
 * Copyright (c) Johannes Knittel
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using ElskeLib.Utils;
using ESkMeansLib.Model;

namespace ESkMeansLib.Tests.datasets
{
    public class TestSet
    {
        public FlexibleVector[]? Data { get; set; }
        public int[]? Labels { get; set; }
        public string? Name { get; set; }

        public static TestSet LoadIris()
        {
            int curLabel = -1;
            var curLabelString = "";
            var dataList = new List<FlexibleVector>();
            var labelsList = new List<int>();
            foreach (var line in File.ReadLines("datasets/iris.data"))
            {
                if(string.IsNullOrWhiteSpace(line))
                    continue;
                var cols = line.Split(',');
                var labelS = cols[^1];
                if (labelS != curLabelString)
                {
                     curLabelString = labelS;
                    curLabel++;
                }

                var v = new FlexibleVector(cols[..^1].Select(s => float.Parse(s, CultureInfo.InvariantCulture))
                    .ToArray());
                
                dataList.Add(v);
                labelsList.Add(curLabel);

            }

            return new TestSet
            {
                Data = dataList.ToArray(),
                Labels = labelsList.ToArray(),
                Name = "Iris"
            };
        }

        public static TestSet Load20Newsgroups()
        {
            const string fn = "datasets/20news.json.gz";
            if (!File.Exists(fn))
            {
                throw new FileNotFoundException("could not find the 20-Newsgroups data set (not distributed with the code)");
            }

            var docs = new List<Document>();

            using (var reader = new StreamReader(new GZipStream(File.OpenRead(fn), CompressionMode.Decompress)))
            {
                string? line;
                while ((line = reader.ReadLine()) != null)
                {
                    if(string.IsNullOrWhiteSpace(line))
                        continue;
                    var doc = JsonSerializer.Deserialize<Document>(line);
                    if(doc != null)
                        docs.Add(doc);
                }
            }

            var elske = KeyphraseExtractor.CreateFromDocuments(docs.Select(d => d.Content));
            elske.StopWords = StopWords.EnglishStopWords;
            var vectors = docs.Select(d =>
            {
                var v = new FlexibleVector(elske.GenerateBoWVector(d.Content));
                v.NormalizeAsUnitVector();
                return v;
            }).ToArray();
            var labelsList = docs.Select(d => d.Label ?? "").Distinct().ToList();
            var labelsDict = new Dictionary<string, int>();
            for (int i = 0; i < labelsList.Count; i++)
            {
                labelsDict.Add(labelsList[i], i);
            }

            var labels = docs.Select(d => labelsDict[d.Label ?? ""]).ToArray();

            return new TestSet
            {
                Data = vectors,
                Labels = labels,
                Name = "20Newsgroups"
            };
        }
    }

    public record Document
    {
        public DateTime Timestamp { get; set; }
        public string? Content { get; set; }
        public string? Label { get; set; }
    }
}
