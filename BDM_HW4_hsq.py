from pyspark import SparkContext
import sys

def createindex(shapefile1):
    import geopandas as gpd
    import rtree
    import fiona.crs
    neighborhoods = gpd.read_file(shapefile1).to_crs(fiona.crs.from_epsg(2263))
    
    indexr = rtree.Rtree()
 
    for idx, geometry in enumerate(neighborhoods.geometry):
        indexr.insert(idx,geometry.bounds)
   
    return (indexr,neighborhoods)


def findzone(dp,pp, indexr, neighborhoods):
    # check pickup point
    
    # check dropoff point
    match_neighbor = indexr.intersection((dp.x,dp.y,dp.x,dp.y))
    # check pickup borough
    match_borough = indexr.intersection((pp.x,pp.y,pp.x,pp.y))
    
    borough_name=None
    
    for idx in match_borough:
        if neighborhoods.geometry[idx].contains(pp):
            borough_name=neighborhoods.borough[idx]
            
    for idx in match_neighbor:
        if neighborhoods.geometry[idx].contains(dp) and borough_name != None:
            return (neighborhoods.neighborhood[idx],borough_name)
            
    return None 

def processTrips(pid,records):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init='epsg:2263',preserve_units=True)
   
    indexr,neighborhoods=createindex('neighborhoods.geojson')
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts={}
    

    for row in reader:
        # pickup point
        
        #if len(row)!=17:
         #   continue
        if len(row) < 17:
            continue
        if  row[10]=='' or row[10]=='NULL' or row[10]==None :
             continue
        pp = geom.Point(proj(float(row[5]),float(row[6])))
        # dropoff point 
        dp = geom.Point(proj(float(row[9]),float(row[10])))
        
        
      
        match = findzone(dp,pp,indexr,neighborhoods)
        if match:
            counts[match] = counts.get(match,0) + 1 
    return counts.items()
 

def takeOrderedByKey(self, num, sortValue = None, reverse=False):
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
        if reverse == True:
            return heapq.nlargest(num, agg, sortValue)
        else:
            return heapq.nsmallest(num, agg, sortValue)              

    return self.combineByKey(init, combine, merge)

if __name__=='__main__':

    fn = 'hdfs:///tmp/bdm/yellow_tripdata_2011-05.csv' if len(sys.argv)<2 else sys.argv[1]
    from pyspark.rdd import RDD
    RDD.takeOrderedByKey = takeOrderedByKey
    sc = SparkContext()
    taxi = sc.textFile(fn)
    counts = taxi.mapPartitionsWithIndex(processTrips) \
            .reduceByKey(lambda x,y: x+y)\
            .map(lambda x:(x[0][1],(x[0][1],x[0][0],x[1])))\
            .takeOrderedByKey(3,sortValue=lambda x: x[2],reverse=True)\
            .flatMap(lambda x:x[1])\
            .map(lambda x:(x[0],[x[1],x[2]]))\
            .reduceByKey(lambda x,y:x+y)\
            .map(lambda x:(x[0],x[1][0],x[1][1],x[1][2],x[1][3],x[1][4],x[1][5]))\
            .sortBy(lambda x: x[0])\
            .saveAsTextFile('hw4output')


   




