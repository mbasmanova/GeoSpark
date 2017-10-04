/**
 * FILE: LineStringJoinTest.java
 * PATH: org.datasyslab.geospark.spatialOperator.LineStringJoinTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialOperator;

import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.Polygon;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.spatialRDD.LineStringRDD;
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
public class LineStringJoinTest extends JoinTestBase {

    private static long expectedMatchCount;

    public LineStringJoinTest(GridType gridType, boolean useLegacyPartitionAPIs) {
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
        initialize("LineStringJoin", "linestring.test.properties");
        expectedMatchCount = Long.parseLong(prop.getProperty("matchCount"));
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }

    /**
     * Test spatial join query with line string RDD.
     *
     * @throws Exception the exception
     */
    @Test
    public void testNestedLoop() throws Exception {

        PolygonRDD queryRDD = createPolygonRDD();
        LineStringRDD spatialRDD = createLineStringRDD();

        partitionRdds(queryRDD, spatialRDD);

        List<Tuple2<Polygon, HashSet<LineString>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedMatchCount, countJoinResults(result));
    }

    /**
     * Test spatial join query with polygon RDD using R tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testRTree() throws Exception {
    	testIndexInt(IndexType.RTREE);
    }

    /**
     * Test spatial join query with polygon RDD using quad tree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testQuadTree() throws Exception {
        testIndexInt(IndexType.QUADTREE);
    }

    private void testIndexInt(IndexType indexType) throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();

        LineStringRDD spatialRDD = createLineStringRDD();

        partitionRdds(queryRDD, spatialRDD);
        spatialRDD.buildIndex(indexType, true);

        List<Tuple2<Polygon, HashSet<LineString>>> result = JoinQuery.SpatialJoinQuery(spatialRDD,queryRDD,false,true).collect();

        sanityCheckJoinResults(result);
        assertEquals(expectedMatchCount, countJoinResults(result));
    }

    @Test
    public void testDynamicRTree() throws Exception {
        testDynamicIndexInt(IndexType.RTREE);
    }

    @Test
    public void testDynamicQuadTree() throws Exception {
        testDynamicIndexInt(IndexType.QUADTREE);
    }

    private void testDynamicIndexInt(IndexType indexType) throws Exception {
        PolygonRDD queryRDD = createPolygonRDD();
        LineStringRDD spatialRDD = createLineStringRDD();

        partitionRdds(queryRDD, spatialRDD);

        JoinQuery.JoinParams joinParams = new JoinQuery.JoinParams(true, indexType);
        List<Tuple2<Polygon, LineString>> results = JoinQuery.spatialJoin(spatialRDD, queryRDD, joinParams).collect();

        sanityCheckFlatJoinResults(results);
        assertEquals(expectedMatchCount, results.size());
    }

    private LineStringRDD createLineStringRDD() {
        return createLineStringRDD(InputLocation);
    }

    private PolygonRDD createPolygonRDD() {
        return createPolygonRDD(InputLocationQueryPolygon);
    }
}