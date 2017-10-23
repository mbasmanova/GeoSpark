/**
 * FILE: PointRDDTest.java
 * PATH: org.datasyslab.geospark.spatialRDD.PointRDDTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialRDD;

import com.vividsolutions.jts.index.SpatialIndex;
import org.apache.spark.storage.StorageLevel;
import org.datasyslab.geospark.enums.GridType;
import org.datasyslab.geospark.enums.IndexType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

// TODO: Auto-generated Javadoc
/**
 * The Class PointRDDTest.
 */
public class PointRDDTest extends SpatialRDDTestBase
{
    /**
     * Once executed before all.
     */
    @BeforeClass
    public static void onceExecutedBeforeAll() {
        initialize(PointRDDTest.class.getSimpleName(), "point.test.properties");
    }

    /**
     * Test constructor.
     *
     * @throws Exception the exception
     */
    /*
        This test case will load a sample data file and
     */
    @Test
    public void testConstructor() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter,true, numPartitions,StorageLevel.MEMORY_ONLY());
        assertEquals(inputCount, spatialRDD.approximateTotalCount);
        assertEquals(inputBoundary, spatialRDD.boundaryEnvelope);
    }

    @Test
    public void testEqualPartitioning() throws Exception {
        testSpatialPartitioning(GridType.EQUALGRID, false);
    }
    
    @Test
    public void testHilbertPartitioning() throws Exception {
        testSpatialPartitioning(GridType.HILBERT, true);
    }
    
    @Test
    public void testRTreePartitioning() throws Exception {
        testSpatialPartitioning(GridType.RTREE, false);
    }

    @Test
    public void testQuadTreePartitioning() throws Exception {
        testSpatialPartitioning(GridType.QUADTREE, false);
    }

    @Test
    public void testKDBTreePartitioning() throws Exception {
        testSpatialPartitioning(GridType.KDBTREE, false);
    }
    
    @Test
    public void testVoronoiPartitioning() throws Exception {
        testSpatialPartitioning(GridType.VORONOI, true);
    }

    private void testSpatialPartitioning(GridType gridType, boolean expectDuplicates) throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,10, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        if (expectDuplicates) {
            assertEquals(spatialRDD.countWithoutDuplicates(), spatialRDD.countWithoutDuplicatesSPRDD());
        } else {
            assertEquals(inputCount, spatialRDD.spatialPartitionedRDD.count());
        }
    }
    
    /**
     * Test build index without set grid.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildIndexWithoutSetGrid() throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,numPartitions,StorageLevel.MEMORY_ONLY());
        spatialRDD.buildIndex(IndexType.RTREE,false);
    }


    /**
     * Test build rtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildRtreeIndex() throws Exception {
        testBuildIndex(IndexType.RTREE);
    }

    /**
     * Test build quadtree index.
     *
     * @throws Exception the exception
     */
    @Test
    public void testBuildQuadtreeIndex() throws Exception {
        testBuildIndex(IndexType.QUADTREE);
    }

    private void testBuildIndex(IndexType indexType) throws Exception {
        PointRDD spatialRDD = new PointRDD(sc, InputLocation, offset, splitter, true,numPartitions, StorageLevel.MEMORY_ONLY());
        spatialRDD.spatialPartitioning(gridType);
        spatialRDD.buildIndex(indexType,true);

        int numPartitions = spatialRDD.indexedRDD.partitions().size();
        assertEquals(spatialRDD.spatialPartitionedRDD.partitions().size(), numPartitions);

        List<SpatialIndex> indexes = spatialRDD.indexedRDD.collect();

        int count = 0;
        for (SpatialIndex index : indexes) {
            List results = index.query(spatialRDD.boundaryEnvelope);
            assertFalse(results.isEmpty());
            count += results.size();
        }
        assertEquals(inputCount, count);
    }

    /**
     * Tear down.
     */
    @AfterClass
    public static void TearDown() {
        sc.stop();
    }
}