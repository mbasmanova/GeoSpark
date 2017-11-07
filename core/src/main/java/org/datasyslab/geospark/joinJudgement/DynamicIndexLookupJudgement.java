/**
 * FILE: DynamicIndexLookupJudgement.java
 * PATH: org.datasyslab.geospark.joinJudgement.DynamicIndexLookupJudgement.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.index.SpatialIndex;
import com.vividsolutions.jts.index.quadtree.Quadtree;
import com.vividsolutions.jts.index.strtree.STRtree;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import org.datasyslab.geospark.enums.IndexType;
import org.datasyslab.geospark.monitoring.GeoSparkMetric;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class DynamicIndexLookupJudgement<T extends Geometry, U extends Geometry>
        extends JudgementBase
        implements FlatMapFunction2<Iterator<T>, Iterator<U>, Pair<U, T>>, Serializable {

    private static final Logger log = LogManager.getLogger(DynamicIndexLookupJudgement.class);

    private final IndexType indexType;
    private final GeoSparkMetric buildCount;
    private final GeoSparkMetric streamCount;
    private final GeoSparkMetric resultCount;
    private final GeoSparkMetric candidateCount;

    /**
     * @see JudgementBase
     */
    public DynamicIndexLookupJudgement(boolean considerBoundaryIntersection, IndexType indexType,
                                       @Nullable DedupParams dedupParams,
                                       GeoSparkMetric buildCount,
                                       GeoSparkMetric streamCount,
                                       GeoSparkMetric resultCount,
                                       GeoSparkMetric candidateCount) {
        super(considerBoundaryIntersection, dedupParams);
        this.indexType = indexType;
        this.buildCount = buildCount;
        this.streamCount = streamCount;
        this.resultCount = resultCount;
        this.candidateCount = candidateCount;
    }

    @Override
    public Iterator<Pair<U, T>> call(final Iterator<T> shapes, final Iterator<U> windowShapes) throws Exception {

        if (!windowShapes.hasNext() || !shapes.hasNext()) {
            buildCount.add(0);
            streamCount.add(0);
            resultCount.add(0);
            return Collections.emptyIterator();
        }

        initPartition();

        final SpatialIndex spatialIndex = buildIndex(windowShapes);

        return new Iterator<Pair<U, T>>() {
            // A batch of pre-computed matches
            private List<Pair<U, T>> batch = null;
            // An index of the element from 'batch' to return next
            private int nextIndex = 0;

            private int shapeCnt = 0;

            @Override
            public boolean hasNext() {
                if (batch != null) {
                    return true;
                } else {
                    return populateNextBatch();
                }
            }

            @Override
            public Pair<U, T> next() {
                if (batch == null) {
                    populateNextBatch();
                }

                if (batch != null) {
                    final Pair<U, T> result = batch.get(nextIndex);
                    nextIndex++;
                    if (nextIndex >= batch.size()) {
                        populateNextBatch();
                        nextIndex = 0;
                    }
                    return result;
                }

                throw new NoSuchElementException();
            }

            private boolean populateNextBatch() {
                if (!shapes.hasNext()) {
                    if (batch != null) {
                        batch = null;
                    }
                    return false;
                }

                batch = new ArrayList<>();

                while (shapes.hasNext()) {
                    shapeCnt ++;
                    streamCount.add(1);
                    final T shape = shapes.next();
                    final List candidates = spatialIndex.query(shape.getEnvelopeInternal());
                    for (Object candidate : candidates) {
                        candidateCount.add(1);
                        final U windowShape = (U) candidate;
                        if (match(windowShape, shape)) {
                            batch.add(Pair.of(windowShape, shape));
                            resultCount.add(1);
                        }
                    }
                    logMilestone(shapeCnt, 100 * 1000, "Streaming shapes");
                    if (!batch.isEmpty()) {
                        return true;
                    }
                }

                batch = null;
                return false;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private SpatialIndex buildIndex(Iterator<U> geometries) {
        long count = 0;
        final SpatialIndex index = newIndex();
        while (geometries.hasNext()) {
            U geometry = geometries.next();
            index.insert(geometry.getEnvelopeInternal(), geometry);
            count++;
        }
        index.query(new Envelope(0.0,0.0,0.0,0.0));
        log("Loaded %d shapes into an index", count);
        buildCount.add((int) count);
        return index;
    }

    private SpatialIndex newIndex() {
        switch (indexType) {
            case RTREE:
                return new STRtree();
            case QUADTREE:
                return new Quadtree();
            default:
                throw new IllegalArgumentException("Unsupported index type: " + indexType);
        }
    }

    private void log(String message, Object...params) {
        if (Level.INFO.isGreaterOrEqual(log.getEffectiveLevel())) {
            final int partitionId = TaskContext.getPartitionId();
            final long threadId = Thread.currentThread().getId();
            log.info("[" + threadId + ", PID=" + partitionId + "] " + String.format(message, params));
        }
    }

    private void logMilestone(long cnt, long threshold, String name) {
        if (cnt > 1 && cnt % threshold == 1) {
            log("[%s] Reached a milestone: %d", name, cnt);
        }
    }
}
