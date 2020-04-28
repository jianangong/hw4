from pyspark import SparkContext
import sys

def createindex(shapefile):
    import geopandas as gpd
    import rtree
    import fiona.crs
    neighbor = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index1 = rtree.Rtree()
    for idx, geometry in enumerate(neighbor.geometry):
        index1.insert(idx,geometry.bounds)
   
    return (index1,neighbor)


def findzone(p, index1, neighbor):
    
    match = index1.intersection((p.x,p.y,p.x,p.y)) 
    for idx in match:
        if neighbor.geometry[idx].contains(p):
            return idx
            
    return None 

def largestN(self, num, sortValue = None):
    import heapq
    def init(a):
        return [a]

    def combine(agg, a):
        agg.append(a)
        return getTopN(agg)

    def merge(a, b):
        agg = a + b
        return getTopN(agg)

    def getTopN(agg):
            return heapq.nlargest(num, agg, sortValue)
         
    return self.combineByKey(init, combine, merge)

def processTrips(pid,records):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:2263',preserve_units=True)
   
    index1,neighbor=createindex('neighborhoods.geojson')
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts={}
    
    for row in reader:
        # pickup point
        if len(row)>=17 and row[10]!='NULL' and row[10]!='':
        
            p1= geom.Point(proj(float(row[5]),float(row[6]))) # pickup point 
        
            p2 = geom.Point(proj(float(row[9]),float(row[10]))) #dropoff point
        # check which borough this pickup point belongs to, which neighborhood the dropoff belongs to
            bo=findzone(p1,index1,neighbor)
            nei=findzone(p2,index1,neighbor)
        

            if bo and nei:
                b_n=(neighbor.borough[bo],neighbor.neighborhood[nei])
                counts[b_n] = counts.get(b_n,0) + 1 
    return counts.items()


 


if __name__=='__main__':

    
    fn = 'hdfs:///tmp/bdm/yellow_tripdata_2011-05.csv' if len(sys.argv)<2 else sys.argv[1]
    from pyspark.rdd import RDD
    RDD.takeOrderedByKey = largestN

    sc = SparkContext()
    taxi = sc.textFile(fn)
    counts=taxi.mapPartitionsWithIndex(processTrips) \
    .reduceByKey(lambda x,y:x+y)\
    .map(lambda x:(x[0][0],(x[0][0],x[0][1],x[1])))\
    .takeOrderedByKey(3,sortValue=lambda x: x[2])\
    .flatMap(lambda x:x[1])\
    .map(lambda x:(x[0],[x[1],x[2]]))\
    .reduceByKey(lambda x,y:x+y)\
    .map(lambda x:(x[0],x[1][0],x[1][1],x[1][2],x[1][3],x[1][4],x[1][5]))\
    .sortBy(lambda x: x[0])\
    .saveAsTextFile(sys.argv[2])


   

   





            


   




