/**
 * FILE: PointFormatMapper.java
 * PATH: org.datasyslab.geospark.formatMapper.PointFormatMapper.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.formatMapper;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.io.WKTReader;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.datasyslab.geospark.enums.FileDataSplitter;
import org.wololo.geojson.Feature;
import org.wololo.geojson.GeoJSONFactory;
import org.wololo.jts2geojson.GeoJSONReader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class PointFormatMapper.
 */
public class PointFormatMapper extends FormatMapper implements FlatMapFunction<Iterator<String>, Point> {


	/**
	 * Instantiates a new point format mapper.
	 *
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PointFormatMapper(FileDataSplitter Splitter, boolean carryInputData) {
		super(Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}

	/**
	 * Instantiates a new point format mapper.
	 *
	 * @param startOffset the start offset
	 * @param endOffset the end offset
	 * @param Splitter the splitter
	 * @param carryInputData the carry input data
	 */
	public PointFormatMapper(Integer startOffset, Integer endOffset, FileDataSplitter Splitter,
			boolean carryInputData) {
		super(startOffset, endOffset, Splitter, carryInputData);
		// TODO Auto-generated constructor stub
	}

    @Override
    public Iterator<Point> call(Iterator<String> stringIterator) throws Exception {
        MultiPoint multiSpatialObjects = null;
        Coordinate coordinate;
        List result= new ArrayList<Point>();
        while (stringIterator.hasNext()) {
            String line = stringIterator.next();
            switch (splitter) {
                case CSV:
                case TSV:
                    lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                    coordinate = new Coordinate(Double.parseDouble(lineSplitList.get(0 + this.startOffset)),
                            Double.parseDouble(lineSplitList.get(1 + this.startOffset)));
                    spatialObject = fact.createPoint(coordinate);
                    if (this.carryInputData) {
                        spatialObject.setUserData(line);
                    }
                    result.add(spatialObject);
                    break;
                case GEOJSON:
                    GeoJSONReader reader = new GeoJSONReader();
                    if (line.contains("Feature")) {
                        Feature feature = (Feature) GeoJSONFactory.create(line);
                        spatialObject = reader.read(feature.getGeometry());
                    } else {
                        spatialObject = reader.read(line);
                    }
                    if (spatialObject instanceof MultiPoint) {
                        /*
                         * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects
                         * and assign original input line to each object.
                         */
                        multiSpatialObjects = (MultiPoint) spatialObject;
                        for (int i = 0; i < multiSpatialObjects.getNumGeometries(); i++) {
                            spatialObject = multiSpatialObjects.getGeometryN(i);
                            if (this.carryInputData) {
                                spatialObject.setUserData(line);
                            }
                            result.add(spatialObject);
                        }
                    } else {
                        if (this.carryInputData) {
                            spatialObject.setUserData(line);
                        }
                        result.add(spatialObject);
                    }
                    break;
                case WKT:
                    lineSplitList = Arrays.asList(line.split(splitter.getDelimiter()));
                    WKTReader wktreader = new WKTReader();
                    spatialObject = wktreader.read(lineSplitList.get(this.startOffset));
                    if (spatialObject instanceof MultiPoint) {
                        multiSpatialObjects = (MultiPoint) spatialObject;
                        for (int i = 0; i < multiSpatialObjects.getNumGeometries(); i++) {
                    /*
                     * If this line has a "Multi" type spatial object, GeoSpark separates them to a list of single objects
                     * and assign original input line to each object.
                     */
                            spatialObject = multiSpatialObjects.getGeometryN(i);
                            if (this.carryInputData) {
                                spatialObject.setUserData(line);
                            }
                            result.add((Point) spatialObject);
                        }
                    } else {
                        if (this.carryInputData) {
                            spatialObject.setUserData(line);
                        }
                        result.add((Point) spatialObject);
                    }
                    break;
            }
        }
        return result.iterator();
    }
}
