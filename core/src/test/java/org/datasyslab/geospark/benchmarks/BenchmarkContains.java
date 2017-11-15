package org.datasyslab.geospark.benchmarks;

import com.esri.core.geometry.ogc.OGCGeometry;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.WKTReader;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkContains {

    @Benchmark
    public void containsJts(BenchmarkData data, Blackhole bh)
        throws Throwable
    {
        bh.consume(data.polygon.contains(data.point));
    }

    @Benchmark
    public void containsEsri(BenchmarkData data, Blackhole bh)
        throws Throwable
    {
        bh.consume(data.ogcPolygon.contains(data.ogcPoint));
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"USA", "AFG"})
        private String name;
        private Geometry polygon;
        private Geometry point;

        private OGCGeometry ogcPolygon;
        private OGCGeometry ogcPoint;

        @Setup
        public void setup() throws Exception
        {
            String wkt = loadWktFromFile();
            WKTReader wktReader = new WKTReader();

            polygon = wktReader.read(wkt);
            point = polygon.getEnvelope().getCentroid();

            ogcPolygon = OGCGeometry.fromText(wkt);
            ogcPoint = OGCGeometry.fromText(point.toText());
        }

        private String loadWktFromFile() throws IOException {
            final String filePath = this.getClass().getClassLoader()
                .getResource(name + "_polygon.wkt").getPath();
            final BufferedReader br = new BufferedReader(new FileReader(filePath));

            StringBuffer sb = new StringBuffer();
            String line = br.readLine();
            br.close();

            return line;
        }
    }

    public static void main(String[] args)
        throws Throwable
    {
        Options options = new OptionsBuilder()
            .verbosity(VerboseMode.NORMAL)
            .include(".*" + BenchmarkContains.class.getSimpleName() + ".*")
            .build();
        new Runner(options).run();
    }
}

