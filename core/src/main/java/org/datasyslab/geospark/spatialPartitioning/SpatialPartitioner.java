/**
 * FILE: SpatialPartitioner.java
 * PATH: org.datasyslab.geospark.spatialPartitioning.SpatialPartitioner.java
 * Copyright (c) 2015-2017 GeoSpark Development Team
 * All rights reserved.
 */
package org.datasyslab.geospark.spatialPartitioning;

import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.List;

// TODO: Auto-generated Javadoc
/**
 * The Class SpatialPartitioner.
 */
public class SpatialPartitioner extends Partitioner implements Serializable{

	/** The num parts. */
	private int numParts;
	private List<Envelope> grids;

	/**
	 * Instantiates a new spatial partitioner.
	 *
	 * @param grids the grids
	 */
	public SpatialPartitioner(List<Envelope> grids)
	{
		// TODO Discard object falling into the overflow partition during partitioning
		this.numParts = grids.size() + 1 /* overflow partition */;
		this.grids = grids;
	}

	public List<Envelope> getGrids() {
		return grids;
	}

	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#getPartition(java.lang.Object)
	 */
	@Override
	public int getPartition(Object key) {
		// TODO Auto-generated method stub
		return (int)key;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.spark.Partitioner#numPartitions()
	 */
	@Override
	public int numPartitions() {
		// TODO Auto-generated method stub
		return numParts;
	}

}
