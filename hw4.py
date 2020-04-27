import fiona
import fiona.crs
import shapely
import rtree
import sys
import pandas as pd
import geopandas as gpd
import csv
import pyproj
import shapely.geometry as geom
from pyspark import SparkContext

proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    

def mapper1(partitionId,records):
    if partitionId==0:
        next(records)
    import csv
    reader=csv.reader(records)
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)   
    
    #import
    neighbor = gpd.read_file('hdfs:///tmp/bdm/neighborhoods.geojsonn').to_crs(fiona.crs.from_epsg(2263))
    
    boroughs = gpd.read_file('hdfs:///tmp/bdm/boroughs.geojson').to_crs(fiona.crs.from_epsg(2263))
    index1 = rtree.Rtree()
    for idx,geometry in enumerate(neighbor.geometry):
        index1.insert(idx, geometry.bounds)
    index2 = rtree.Rtree()
    for idx,geometry in enumerate(boroughs.geometry):
        index2.insert(idx, geometry.bounds)
    for row in reader:
        p = geom.Point(proj(float(row[3]), float(row[2])))
        for idx1 in index1.intersection((p.x, p.y, p.x, p.y)):
            # idx is in the list of shapes that might match
            if neighbor.geometry[idx1].contains(p):
                for idx2 in index2.intersection((neighbor.geometry[idx1].bounds)):
                    if boroughs.geometry[idx2].contains(p):
                        yield ((idx2,idx1),1)
                        
if __name__ == '__main__':                        
    sc = SparkContext()
    yellow_data = sys.argv[1]
    taxi = sc.textFile(yellow_data).cache()   

    count=taxi.mapPartitionsWithIndex(mapper1)\
        .reduceByKey(lambda x,y:x+y)\
         .sortBy(keyfunc=(lambda x:x[1]),ascending=False)

    count.map(lambda x:(boroughs['boro_name'][x[0][0]],(neighbor['neighborhood'][x[0][1]],x[1])))\
    .reduceByKey(lambda x,y:x+y).map(lambda x: (x[0],x[1][:6])).sortByKey()\
    .map(lambda x:(x[0],x[1][0],x[1][1],x[1][2],x[1][3],x[1][4],x[1][5]))\
    .saveAsTextFile(sys.argv[2])