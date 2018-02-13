
/* Video LINK: https://www.youtube.com/watch?v=9Ic1qk5zLyw&t=80s&list=PLW1_iURPRr2DTTIJq51diOhknq9gy3I7O&index=2              */

import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.spatialOperator.RangeQuery; 
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.enums.FileDataSplitter;
import com.vividsolutions.jts.geom.Envelope;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialOperator.KNNQuery;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Coordinate;
import org.datasyslab.geospark.spatialOperator.JoinQuery;
import org.datasyslab.geospark.spatialRDD.CircleRDD;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;



/* 1. Start an example Spatial Range Query without Index ----------------------------*/
val queryEnvelope=new Envelope (35.08,32.99,-113.79,-109.73);
val queryEnvelope=new Envelope (-113.79,-109.73,32.99,35.08);
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, false, false).count();



/* 2. Start an example Spatial Range Query with Index ----------------------------*/
val queryEnvelope=new Envelope (-113.79,-109.73,32.99,35.08);
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
objectRDD.buildIndex(IndexType.RTREE,false);
val resultSize = RangeQuery.SpatialRangeQuery(objectRDD, queryEnvelope, false, true).count();



/*3. Start an example Spatial KNN Query without Index ----------------------------*/
val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-109.73, 35.08));
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val resultSize = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5,false).size();



/*4. Start an example Spatial KNN Query with Index ----------------------------*/
val fact=new GeometryFactory();
val queryPoint=fact.createPoint(new Coordinate(-109.73, 35.08));
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
objectRDD.buildIndex(IndexType.RTREE,false);
val resultSize = KNNQuery.SpatialKnnQuery(objectRDD, queryPoint, 5,true).size();



/*5. Start an example Spatial Join Query without Index ----------------------------*/
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val rectangleRDD = new RectangleRDD(sc, "hdfs://master:54310/dataset/zcta510.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY);
objectRDD.spatialPartitioning(GridType.RTREE);
rectangleRDD.spatialPartitioning(objectRDD.grids);
val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,rectangleRDD,false,false).count();



/*6. Start an example Spatial Join Query with Index ----------------------------*/
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val rectangleRDD = new RectangleRDD(sc, "hdfs://master:54310/dataset/zcta510.csv", 0, FileDataSplitter.CSV, false); 
objectRDD.spatialPartitioning(GridType.RTREE);
objectRDD.buildIndex(IndexType.RTREE,true);
rectangleRDD.spatialPartitioning(objectRDD.grids);
val resultSize = JoinQuery.SpatialJoinQuery(objectRDD,rectangleRDD,true, false).count();



/*7. Start an example Distance Join Query without Index ----------------------------*/
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY);
val centerGeometryRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY);
val queryRDD = new CircleRDD(centerGeometryRDD,0.1);
objectRDD.spatialPartitioning(GridType.RTREE);
queryRDD.spatialPartitioning(objectRDD.grids);
val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryRDD,false,false).count();



/*8. Start an example Distance Join Query with Index ----------------------------*/
val objectRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val centerGeometryRDD = new PointRDD(sc, "hdfs://master:54310/dataset/arealm.csv", 0, FileDataSplitter.CSV, false, StorageLevel.MEMORY_ONLY); 
val queryRDD = new CircleRDD(centerGeometryRDD,0.1);
objectRDD.spatialPartitioning(GridType.RTREE);
objectRDD.buildIndex(IndexType.RTREE,true);
queryRDD.spatialPartitioning(objectRDD.grids);
val resultSize = JoinQuery.DistanceJoinQuery(objectRDD,queryRDD,true, false).count();
