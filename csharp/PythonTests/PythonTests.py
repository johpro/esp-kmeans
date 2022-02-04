from clr_loader import get_coreclr
from pythonnet import set_runtime
import os
#we have to load the right .NET runtime (>= 6.0)
rt = get_coreclr("test.runtimeconfig.json")
set_runtime(rt)
#then we have to add a reference to the library
import clr
dll_path = os.path.abspath("../ESPkMeansLib/bin/Release/net6.0/ESPkMeansLib.dll")
clr.AddReference(dll_path)
#now we can import classes from the library
from ESPkMeansLib import KMeans
#it is also possible to import other .NET types
from System import Array, Single, Int32, ValueTuple

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
