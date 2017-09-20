/**
 * FILE: PolygonJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.PolygonJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.Polygon;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.PolygonRDD;
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
public class PolygonJoinTest extends JoinTestBase {

    static long expectedContainsMatchCount;
    static long expectedIntersectsMatchCount;

    public PolygonJoinTest(GridType gridType, boolean useLegacyPartitionAPIs) {
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
            { GridType.QUADTREE, false },
        });
    }

    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize("PolygonJoin", "polygon.test.properties");

        expectedContainsMatchCount = Long.parseLong(prop.getProperty("containsMatchCount"));
        expectedIntersectsMatchCount = Long.parseLong(prop.getProperty("intersectsMatchCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    @Test
    public void testDynamicRTreeAndContains() throws Exception {
        testDynamicIndexInt(false, IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTreeAndContains() throws Exception {
        testDynamicIndexInt(false, IndexType.QUADTREE);
    }

    @Test
    public void testDynamicRTreeAndIntersects() throws Exception {
        testDynamicIndexInt(true, IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTreeAndIntersects() throws Exception {
        testDynamicIndexInt(true, IndexType.QUADTREE);
    }

    private void testDynamicIndexInt(boolean intersects, IndexType indexType) throws Exception {
        final PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        final PolygonRDD spatialRDD = createPolygonRDD(InputLocation);
        partitionRdds(queryRDD, spatialRDD);

        final JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(intersects, indexType);
        final List<Tuple2<Polygon, Polygon>> results = JoinQuery.spatialJoin(spatialRDD, queryRDD, joinParams).collect();
        sanityCheckFlatJoinResults(results);

        assertEquals(getExpectedCount(intersects), results.size());
    }

    /**
     * Test spatial join query with polygon RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoopAndContains() throws Exception {
        testNestedLoopInt(false);
    }

    @Test
    public void testNestedLoopAndIntersects() throws Exception {
        testNestedLoopInt(true);
    }

    private void testNestedLoopInt(boolean intersects) throws Exception {
        PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        PolygonRDD spatialRDD = createPolygonRDD(InputLocation);

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,intersects).collect();
        sanityCheckJoinResults(result);
        assertEquals(getExpectedCount(intersects), countJoinResults(result));
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTreeAndContains() throws Exception {
        testIndexInt(false, IndexType.RTREE);
    }

    @Test
    public void testRTreeAndIntersects() throws Exception {
        testIndexInt(true, IndexType.RTREE);
    }

    @Test
    public void testQuadTreeAndContains() throws Exception {
        testIndexInt(false, IndexType.QUADTREE);
    }

    @Test
    public void testQuadTreeAndIntersects() throws Exception {
        testIndexInt(true, IndexType.QUADTREE);
    }

    private void testIndexInt(boolean intersects, IndexType indexType) throws Exception {
        PolygonRDD queryRDD = createPolygonRDD(InputLocationQueryPolygon);
        PolygonRDD spatialRDD = createPolygonRDD(InputLocation);

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, HashSet<Polygon>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,true,intersects).collect();
        sanityCheckJoinResults(result);
        assertEquals(getExpectedCount(intersects), countJoinResults(result));
    }

    private long getExpectedCount(boolean intersects) {
        return intersects ? expectedIntersectsMatchCount : expectedContainsMatchCount;
    }
}