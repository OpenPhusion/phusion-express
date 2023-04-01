package cloud.phusion.test;

import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import cloud.phusion.express.util.TimeMarker;
import org.junit.*;

import java.util.Properties;

public class IDGeneratorTest {

    @Test
    public void testGenerateID() throws Exception {
        Properties props = new Properties();
        props.setProperty(EngineFactory.Cluster_DataCenter, "1");
        props.setProperty(EngineFactory.Cluster_Worker, "1");
        Engine engine = EngineFactory.createEngine(props);

        TimeMarker marker = new TimeMarker();

        long l = engine.generateUniqueId(null);
        double ms = marker.mark();

        System.out.println(String.format("ID: %d, Time: %.1fms", l, ms));
    }

}
