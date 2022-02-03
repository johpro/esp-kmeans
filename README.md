# ES-kMeans
ES-kMeans is a fast and easy-to-use clustering library written in C# to cluster high-dimensional and potentially sparse data with k-Means++ or Spherical k-Means++ (Spherical k-Means uses the cosine distance instead of the Euclidean).

The k-Means algorithm belongs to one of the most popular clustering algorithms, but it typically does not scale well (i.e., linearly) with *k*, the number of clusters. The goal of this library is to cluster large datasets efficiently even if the number of clusters is high. It has a highly parallel implementation that utilizes AVX instructions (if applicable) and applies several optimizations to reduce the number of comparisons. For instance, the Spherical k-Means implementation achieves sublinear scaling with respect to the number of clusters if applied to sparse data (e.g., text documents). If you want to find out more how this works, you can read the paper [Efficient Sparse Spherical k-Means for Document Clustering](https://arxiv.org/abs/2108.00895) that details some of the applied strategies.

## Setup

The most convenient way of using the library is to get it on NuGet.

## Getting Started

The following code runs k-Means++ on the provided data (composed of four two-dimensional vectors):

```csharp
var km = new KMeans();
var data = new[]
{
    new[] { 0.1f, 0.8f },
    new[] { 0.2f, 0.7f },
    new[] { 0.5f, 0.45f },
    new[] { 0.6f, 0.5f }
};
//cluster data into two clusters with k-Means++
var (clustering, centroids) = km.Cluster(data, 2);
/* OUTPUT:
 * clustering: 0,0,1,1
 * centroids: [[0.15, 0.75], [0.55, 0.475]] */
```

It is also possible to let the clustering run several times. The function will then return the "best" result, that is, the clustering with the lowest distances between data points and their assigned cluster centroid. The following call would run the clustering algorithm five times:
```csharp
(clustering, centroids) = km.Cluster(data, 2, 5);
```

The resulting centroids are instances of the `FlexibleVector` class which represents dense or sparse vectors:
```csharp
var val = centroids[0][0]; //get value at index 0 of first centroid (0.15)
```
However, this library was specifically designed to handle sparse data, that is, data vectors which are very high-dimensional but typically only have a few non-zero entries. Such vectors are internally stored as list of index-value pairs. 
```csharp
//get value at index 0 of first centroid (0.15):
var val = centroids[0][0];
//get all stored values:
var vals = centroids[0].Values;
//get the list of corresponding indexes of the stored value (for dense vectors this is empty):
var indexes = centroids[0].Indexes; 
//convert dense centroid vector into sparse representation:
var sparseVec = centroids[0].ToSparse();
//convert it back into dense representation (we have to specify the dimension of the vector):
var denseVec = sparseVec.ToDense(2);
```
If we run the clustering on sparse data, the resulting centroids are also sparse:
```csharp
//sparse data specified with index,value pairs
var sparseData = new[]
{
    new[] { (0, 0.1f), (3, 0.8f), (7, 0.1f) },
    new[] { (0, 0.2f), (3, 0.8f), (6, 0.05f) },
    new[] { (0, 0.5f), (3, 0.45f) },
    new[] { (0, 0.6f), (3, 0.5f) }
};
//cluster sparse data into two clusters and use cosine distance (Spherical k-Means)
km.UseSphericalKMeans = true;
(clustering, centroids) = km.Cluster(sparseData, 2);
/* OUTPUT:
 * clustering: 0,0,1,1
 * centroids: [[(0, 0.18335475), (3, 0.9806314), (7, 0.0618031), (6, 0.030387914)],
 *             [(0, 0.7558947), (3, 0.6546932)]]
 */
```
The previous example used Spherical k-Means that applies the cosine distance instead of the Euclidean. Spherical k-Means is particularly useful for clustering sparse text representations (e.g., "bag-of-words" model). The following example uses the [ELSKE](https://github.com/johpro/elske) library ([get ElskeLib on NuGet](https://www.nuget.org/packages/ElskeLib/)) to obtain such vector representations of a handful of documents so that we can run the clustering algorithm on them:
```csharp
var documents = new[]
{
    "I went shopping for groceries and also bought tea",
    "This hotel is amazing and the view is perfect",
    "My shopping heist resulted in lots of new shoes",
    "The rooms in this hotel are a bit dirty"
};
//obtain sparse vector representations using ElskeLib
var elske = KeyphraseExtractor.CreateFromDocuments(documents);
elske.StopWords = StopWords.EnglishStopWords;
var docVectors = documents
    .Select(doc => elske.GenerateBoWVector(doc, true));
    
//run clustering
km.UseSphericalKMeans = true;
(clustering, centroids) = km.Cluster(docVectors, 2);
//output of clustering: 0,1,0,1

//use centroids to determine most relevant tokens for each cluster
for (int i = 0; i < centroids.Length; i++)
{
    var c = centroids[i];
    //get the two entries with the highest weight and retrieve corresponding word
    var words = c.AsEnumerable()
        .OrderByDescending(p => p.value)
        .Take(2)
        .Select(p => elske.ReferenceIdxMap.GetToken(p.key));
    Trace.WriteLine($"cluster {i}: {string.Join(',', words)}");
}
/*
 * OUTPUT:
 * cluster 0: groceries,bought
 * cluster 1: hotel,amazing
 */
```

## License
ES-kMeans is MIT licensed.

If you use the library as part of your work, it would be great if you cite the following paper:

	@inproceedings{Knittel21SKMeans,
	author = {Knittel, Johannes and Koch, Steffen and Ertl, Thomas},
	title = {Efficient Sparse Spherical K-Means for Document Clustering},
	year = {2021},
	isbn = {9781450385961},
	publisher = {Association for Computing Machinery},
	address = {New York, NY, USA},
	url = {https://doi.org/10.1145/3469096.3474937},
	doi = {10.1145/3469096.3474937},
	booktitle = {Proceedings of the 21st ACM Symposium on Document Engineering},
	articleno = {6},
	numpages = {4},
	keywords = {document clustering, k-means, large-scale analysis},
	location = {Limerick, Ireland},
	series = {DocEng '21}
	}
