/**
 * FILE: CircleTest.java
 * PATH: org.datasyslab.geospark.geometryObjects.CircleTest.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.geometryObjects;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

// TODO: Auto-generated Javadoc
/**
 * The Class CircleTest.
 */
public class CircleTest {
    /** The geom fact. */
    private static GeometryFactory geomFact = new GeometryFactory();
    private static WKTReader wktReader = new WKTReader();

	/**
     * Test get center.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetCenter() throws Exception 
    {
        Circle circle = new Circle(makePoint(0.0,0.0), 0.1);
        assertEquals(makePoint(0.0,0.0).getCoordinate(), circle.getCenterPoint());
    }

    /**
     * Test get radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetRadius() throws Exception {
        Circle circle = new Circle(makePoint(0.0,0.0), 0.1);
        assertEquals(0.1, circle.getRadius(), 0.01);
    }

    /**
     * Test set radius.
     *
     * @throws Exception the exception
     */
    @Test
    public void testSetRadius() throws Exception {
        Circle circle = new Circle(makePoint(0.0,0.0), 0.1);
        circle.setRadius(0.2);
        assertEquals(circle.getRadius(), 0.2, 0.01);
    }

    /**
     * Test get MBR.
     *
     * @throws Exception the exception
     */
    @Test
    public void testGetMBR() throws Exception {
        Circle circle = new Circle(makePoint(0.0,0.0), 0.1);
        assertEquals(new Envelope(-0.1, 0.1, -0.1, 0.1), circle.getEnvelopeInternal());
    }

    /**
     * Test contains.
     *
     * @throws Exception the exception
     */
    @Test
    public void testCovers() throws Exception {
        Circle circle = new Circle(makePoint(0.0,0.0), 0.5);

        assertTrue(circle.covers(makePoint(0.0, 0.0)));
        assertTrue(circle.covers(makePoint(0.1, 0.2)));
        assertFalse(circle.covers(makePoint(0.4, 0.4)));
        assertFalse(circle.covers(makePoint(-1, 0.4)));

        assertTrue(circle.covers(parseWkt("POLYGON ((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1))")));
        assertFalse(circle.covers(parseWkt("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")));

        assertTrue(circle.covers(parseWkt("LINESTRING (-0.1 0, 0.2 0.3)")));
        assertFalse(circle.covers(parseWkt("LINESTRING (-0.1 0, 0 1)")));
    }

    /**
     * Test intersects.
     *
     * @throws Exception the exception
     */
    @Test
    public void testIntersects() throws Exception {
        Circle circle = new Circle(makePoint(0.0,0.0), 0.5);
        assertTrue(circle.intersects(makePoint(0, 0)));
        assertTrue(circle.intersects(makePoint(0.1, 0.2)));
        assertFalse(circle.intersects(makePoint(0.4, 0.4)));
        assertFalse(circle.intersects(makePoint(-1, 0.4)));

        assertTrue(circle.intersects(parseWkt("POLYGON ((-0.1 0.1, 0 0.4, 0.1 0.2, -0.1 0.1))")));
        assertTrue(circle.intersects(parseWkt("POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))")));
        assertFalse(circle.intersects(parseWkt("POLYGON ((1 1, 1 2, 2 2, 1 1))")));
        assertFalse(circle.intersects(parseWkt("POLYGON ((0.4 0.4, 1 2, 2 2, 0.4 0.4))")));

        assertTrue(circle.intersects(parseWkt("LINESTRING (-0.1 0, 0 1)")));
    }
    
    /**
     * Test equality.
     */
    @Test
    public void testEquality()
    {
        assertEquals(
            new Circle(makePoint(-112.574945, 45.987772), 0.01),
            new Circle(makePoint(-112.574945, 45.987772), 0.01));

        assertNotEquals(
            new Circle(makePoint(-112.574945, 45.987772), 0.01),
            new Circle(makePoint(-112.574942, 45.987772), 0.01));
    }

    private Point makePoint(double x, double y) {
        return geomFact.createPoint(new Coordinate(x, y));
    }

    private Geometry parseWkt(String wkt) throws ParseException {
        return wktReader.read(wkt);
    }
}