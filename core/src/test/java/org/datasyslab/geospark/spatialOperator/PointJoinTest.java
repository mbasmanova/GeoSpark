/**
 * FILE: PointJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.PointJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PointRDD;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
import org.datasyslab.geospark.spatialRDD.RectangleRDD;
import org.datasyslab.geospark.spatialRDD.SpatialRDD;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Arizona State University DataSystems Lab
 */
@RunWith(Parameterized.class)
public class PointJoinTest extends JoinTestBase {

    private static long expectedRectangleMatchCount;
    private static long expectedPolygonMatchCount;

    public PointJoinTest(GridType gridType, boolean useLegacyPartitionAPIs) {
        super(gridType, useLegacyPartitionAPIs);
    }

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][] {
            { GridType.RTREE, true },
            { GridType.RTREE, false },
            { GridType.QUADTREE, true },
            { GridType.QUADTREE, false},
            { GridType.HILBERT, true },
            { GridType.HILBERT, false },
        });
    }

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
    	initialize("PointJoin", "point.test.properties");
        expectedRectangleMatchCount = Long.parseLong(prop.getProperty("rectangleMatchCount"));
        expectedPolygonMatchCount = Long.parseLong(prop.getProperty("polygonMatchCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    /**
     * Test spatial join query.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopWithRectanges() throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        testNestedLoopInt(queryRDD, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopWithPolygons() throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();
        testNestedLoopInt(queryRDD, expectedPolygonMatchCount);
    }

    private void testNestedLoopInt(SpatialRDD<Polygon> queryRDD, long expectedCount) throws Exception {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with rectangle RDD using rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithRectanges() throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        testIndexInt(queryRDD, IndexType.RTREE, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeWithPolygons() throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();
        testIndexInt(queryRDD, IndexType.RTREE, expectedPolygonMatchCount);
    }

    /**
     * Test spatial join query with rectangle RDD using quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTreeWithRectanges() throws Exception {
        RectangleRDD queryRDD = createRectangleRDD();
        testIndexInt(queryRDD, IndexType.QUADTREE, expectedRectangleMatchCount);
    }

    /**
     * Test spatial join query with polygon RDD using quad tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTreeWithPolygons() throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();
        testIndexInt(queryRDD, IndexType.QUADTREE, expectedPolygonMatchCount);
    }

    private void testIndexInt(SpatialRDD<Polygon> queryRDD, IndexType indexType, long expectedCount) throws Exception {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, HashSet<Point>>> result = JoinQuery.SpatialJoinQuery(spatialRDD, queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedCount, countJoinResults(result));
    }

    @Test
    public void testDynamicRTreeWithRectanges() throws Exception {
        final RectangleRDD rectangleRDD = createRectangleRDD();
        testDynamicRTreeInt(rectangleRDD, IndexType.RTREE, expectedRectangleMatchCount);
    }

    private void testDynamicRTreeInt(RectangleRDD queryRDD, IndexType indexType, long expectedCount) throws Exception {
        PointRDD spatialRDD = createPointRDD();

        partitionRdds(queryRDD, spatialRDD);

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, indexType);
        List<Tuple2<Polygon, Point>> results = JoinQuery.spatialJoin(spatialRDD, queryRDD, joinParams).collect();

        sanityCheckFlatJoinResults(results);
        assertEquals(expectedCount, results.size());
    }

    private RectangleRDD createRectangleRDD() {
        return createRectangleRDD(InputLocationQueryWindow);
    }

    private PolygonRDD createPolygonRDD() {
        return createPolygonRDD(InputLocationQueryPolygon);
    }

    private PointRDD createPointRDD() {
        return createPointRDD(InputLocation);
    }
}