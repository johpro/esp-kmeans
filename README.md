
# ES-kMeans
ES-kMeans is a fast and easy-to-use clustering library written in C# to cluster high-dimensional and potentially sparse data with k-Means++ or Spherical k-Means++ (Spherical k-Means uses the cosine distance instead of the Euclidean).

The k-Means algorithm belongs to one of the most popular clustering algorithms, but it typically does not scale well (i.e., linearly) with *k*, the number of clusters. The goal of this library is to cluster large datasets efficiently even if the number of clusters is high. It has a highly parallel implementation that utilizes AVX instructions (if applicable) and applies several optimizations to reduce the number of comparisons. For instance, the Spherical k-Means implementation achieves sublinear scaling with respect to the number of clusters if applied to sparse data (e.g., text documents). If you want to find out more how this works, you can read the paper [Efficient Sparse Spherical k-Means for Document Clustering](https://arxiv.org/abs/2108.00895) that details some of the applied strategies.

## Setup

The most convenient way of using the library is to install the package from [NuGet](https://www.nuget.org/packages/ESkMeansLib).

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
    "The rooms in this hotel are a bit dirty",
    "my three fav things to do: shopping, shopping, shopping"
};
//obtain sparse vector representations using ElskeLib
var elske = KeyphraseExtractor.CreateFromDocuments(documents);
elske.StopWords = StopWords.EnglishStopWords;
var docVectors = documents
    .Select(doc => elske.GenerateBoWVector(doc, true));
    
//run clustering three times
km.UseSphericalKMeans = true;
(clustering, centroids) = km.Cluster(docVectors, 2, 3);
//output of clustering: 1,0,1,0,1

//use centroids to determine most relevant tokens for each cluster
for (int i = 0; i < centroids.Length; i++)
{
    var c = centroids[i];
    //we can regard each centroid as a weighted word list
    //get the two entries with the highest weight and retrieve corresponding word
    var words = c.AsEnumerable()
        .OrderByDescending(p => p.value)
        .Take(2)
        .Select(p => elske.ReferenceIdxMap.GetToken(p.key));
    Trace.WriteLine($"cluster {i}: {string.Join(',', words)}");
}
/*
* OUTPUT:
* cluster 0: hotel,amazing
* cluster 1: shopping,groceries
*/
```

## Python Examples
The library targets .NET 6 onwards, but thanks to the [pythonnet](https://github.com/pythonnet/pythonnet) project you can also call it from your python code. Make sure that you have installed the [.NET runtime](https://dotnet.microsoft.com/en-us/download) (>= 6) and download the library (ESkMeansLib.dll).

Install the required python packages (>= 3):

	pip install pythonnet==3.0.0a2 clr-loader

Create a file `runtimeconfig.json` that specifies the correct .NET runtime to use (you may have to adapt the version to your specific environment):

	{
	  "runtimeOptions": {
	    "tfm": "net6.0",
	    "framework": {
	      "name": "Microsoft.NETCore.App",
	      "version": "6.0.1"
	    }
	  }
	}

We can now import the library in the python script:

```python
from clr_loader import get_coreclr
from pythonnet import set_runtime
import os
#we have to load the right .NET runtime (>= 6.0)
rt = get_coreclr("runtimeconfig.json")
set_runtime(rt)
#then we have to add a reference to the library
import clr
#absolute path of the ESkMeansLib library
dll_path = os.path.abspath("./path/to/ESkMeansLib.dll")
clr.AddReference(dll_path)
#now we can import classes from the library
from ESkMeansLib import KMeans
#it is also possible to import other .NET types
from System import Array, Single, Int32, ValueTuple
```

The following code runs k-Means++ on the provided data (composed of four two-dimensional vectors):

```python
#===== RUN k-Means++ ON DENSE DATA =====

#define data by converting python list to .net array
data = Array[Array[Single]]([[0.1, 0.8],
                             [0.2, 0.7],
                             [0.5, 0.45],
                             [0.6, 0.5]])
#instantiate KMeans class
km = KMeans()
#run clustering on data
res = km.Cluster(data, 2)
#unpack returned values from ValueTuple
clustering = res.Item1
centroids = res.Item2
#clustering contains the zero-based cluster association
#of the data as a .net array of type Int32[]
print(f"clustering.length: {clustering.Length}")
print(f"first item in clustering: {clustering[0]}")
#we can also convert it to an actual Python list
print(f"clustering as python list: {list(clustering)}")
#centroids is a .net array of type FlexibleVector[]
#and contains the calculated centroids of each cluster
print(f"centroids.Length: {centroids.Length}")
for i in range(centroids.Length):
    print(f"centroid {i}: {centroids[i]}")
#convert dense vector to sparse vector
sparse = centroids[0].ToSparse()
arr = list(sparse.AsEnumerable())
```

ES-kMeans was specifically designed to handle sparse data, that is, data vectors which are very high-dimensional but typically only have a few non-zero entries. Such vectors are internally stored as list of index-value pairs. Calling the method on sparse data will also return sparse centroids:

```python
#===== RUN Spherical k-Means++ ON SPARSE DATA =====

#define sparse data as array of index-value pairs
data = Array[Array[ValueTuple[Int32, Single]]]([
    [ ValueTuple[Int32, Single](0, 0.1), ValueTuple[Int32, Single](3, 0.8), ValueTuple[Int32, Single](7, 0.1) ],
    [ ValueTuple[Int32, Single](0, 0.2), ValueTuple[Int32, Single](3, 0.8), ValueTuple[Int32, Single](6, 0.05) ],
    [ ValueTuple[Int32, Single](0, 0.5), ValueTuple[Int32, Single](3, 0.45) ],
    [ ValueTuple[Int32, Single](0, 0.6), ValueTuple[Int32, Single](3, 0.5) ]
    ])
#set to Spherical k-Means
km.UseSphericalKMeans = True
#run clustering
res = km.Cluster(data, 2)
#unpack returned values from ValueTuple
clustering = res.Item1
centroids = res.Item2
#print clustering and centroids
print(f"clustering: {list(clustering)}")
print(f"centroids.Length: {centroids.Length}")
for i in range(centroids.Length):
    print(f"centroid {i}: {centroids[i]}")
```

The previous example used Spherical k-Means that applies the cosine distance instead of the Euclidean. Spherical k-Means is particularly useful for clustering sparse text representations (e.g., "bag-of-words" model). The following example uses the [ELSKE](https://github.com/johpro/elske) library to obtain such vector representations of a handful of documents so that we can run the clustering algorithm on them:

```python
#===== RUN Spherical k-Means++ ON TEXT DATA USING ELSKE =====

#add reference to ElskeLib and import class
dll_path = os.path.abspath("./ElskeLib.dll")
clr.AddReference(dll_path)
#now we can import classes from the library
from ElskeLib.Utils import KeyphraseExtractor, StopWords
from System import String

documents = Array[String](["I went shopping for groceries and also bought tea",
    "This hotel is amazing and the view is perfect",
    "My shopping heist resulted in lots of new shoes",
    "The rooms in this hotel are a bit dirty",
    "my three fav things to do: shopping, shopping, shopping"])

#create KeyphraseExtractor instance from a reference collection
#(here, we just use actual documents that we want to cluster)
elske = KeyphraseExtractor.CreateFromDocuments(documents)
#we get better vector representations if we ignore common stop words
elske.StopWords = StopWords.EnglishStopWords;
#convert each document into a sparse vector representation
docVectors = Array[Array[ValueTuple[Int32, Single]]]([ elske.GenerateBoWVector(doc, True) for doc in documents])

#run clustering three times
km.UseSphericalKMeans = True;
res = km.Cluster(docVectors, 2, 3);
#unpack returned values from ValueTuple
clustering = res.Item1
centroids = list(res.Item2)
#print clustering
print(f"clustering: {list(clustering)}")

for i in range(len(centroids)):    
    c = centroids[i]
    #we can interpret centroid as weighted word list
    words = list(c.AsEnumerable())
    #sort indexes=words by their weight
    words.sort(key=lambda t: float(t.Item2))
    #get the two most relevant indexes and obtain their corresponding word
    w1 = elske.ReferenceIdxMap.GetToken(words[-1].Item1)
    w2 = elske.ReferenceIdxMap.GetToken(words[-2].Item1)
    print(f"cluster {i}: {w1}, {w2}")
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
