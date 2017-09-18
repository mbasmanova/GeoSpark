package org.datasyslab.geospark.joinJudgement;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import org.datasyslab.geospark.utils.ReferencePointUtils;

import java.io.Serializable;

abstract class JudgementBase implements Serializable {
    private final boolean considerBoundaryIntersection;
    private final HalfOpenRectangle extent;

    protected JudgementBase(boolean considerBoundaryIntersection) {
        this(considerBoundaryIntersection, null);
    }

    protected JudgementBase(boolean considerBoundaryIntersection, Envelope extent) {
        this.considerBoundaryIntersection = considerBoundaryIntersection;
        this.extent = new HalfOpenRectangle(extent);
    }

    protected boolean match(Geometry polygon, Geometry geometry) {
        if (!geoMatch(polygon, geometry)) {
            return false;
        }

        if (extent == null) {
            return true;
        }

        // Handle easy case of points. Each point is assigned to exactly one partition.
        // Hence, join result cannot have duplicates.
        if (geometry instanceof Point) {
            return true;
        }

        // For more complex geometries, check if reference point of the intersection lies
        // within the extent of this partition.
        final Geometry intersection = polygon.intersection(geometry);
        final Point refPoint = ReferencePointUtils.getReferencePoint(intersection);
        return extent.contains(refPoint);
    }

    private boolean geoMatch(Geometry polygon, Geometry geometry) {
        return considerBoundaryIntersection ? polygon.intersects(geometry) : polygon.covers(geometry);
    }
}
