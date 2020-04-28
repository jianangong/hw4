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


def findzone(p1,p2, index1, neighbor):
    # check pickup point
    
    
    pickid = index1.intersection((p1.x,p1.y,p1.x,p1.y)) # check pickup boroug
    dropid = index1.intersection((p2.x,p2.y,p2.x,p2.y)) # check dropoff point
    borough_name=None
    
    for idx in pickid:
        if neighbor.geometry[idx].contains(p1):
            borough_name=neighbor.borough[idx]
            
    for idx in dropid:
        if neighbor.geometry[idx].contains(p2)  and borough_name != None:
            return (borough_name,neighbor.neighborhood[idx])
            
    return None 

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
        if len(row)>=17 and row[10]!='NULL' and row[10]!=None and row[10]!='':
        
            p1= geom.Point(proj(float(row[5]),float(row[6]))) # pickup point 
        
            p2 = geom.Point(proj(float(row[9]),float(row[10]))) #dropoff point
        # check which borough this pickup point belongs to, which neighborhood the dropoff belongs to
        
      
            match = findzone(p1,p2,index1,neighbor)

            if match:
                counts[match] = counts.get(match,0) + 1 
    return counts.items()
 


if __name__=='__main__':

    
    fn = 'hdfs:///tmp/bdm/yellow_tripdata_2011-05.csv' if len(sys.argv)<2 else sys.argv[1]
    from pyspark.rdd import RDD
 
    sc = SparkContext()
    taxi = sc.textFile(fn)
    taxi.mapPartitionsWithIndex(processTrips) \
    .reduceByKey(lambda x,y:x+y)\
    .sortBy(keyfunc=(lambda x:x[1]),ascending=False)\
    .sortBy(lambda x:x[0][0])\
    .map(lambda x:(x[0][0],(x[0][1],x[1])))\
    .reduceByKey(lambda x,y:x+y)\
    .map(lambda x: (x[0],x[1][:6]))\
    .saveAsTextFile(sys.argv[2])
            


   




