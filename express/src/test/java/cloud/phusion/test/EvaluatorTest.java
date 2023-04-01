package cloud.phusion.test;

import cloud.phusion.Engine;
import cloud.phusion.EngineFactory;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class EvaluatorTest {

    @Test
    public void testCondition() throws Exception {
        Engine engine = EngineFactory.createEngine();

        String condition = "{" +
                "  \"expression\": \"(x==3.0 || x.equals(e)) && c.endsWith(d)\"," +
                "  \"vars\": {" +
                "    \"x\": {\"type\":\"Float\", \"fromMessage\":\"a.b\"}," +
                "    \"c\": {\"type\":\"String\", \"fromMessage\":\"c\"}," +
                "    \"d\": {\"type\":\"String\", \"fromConfig\":\"d\"}," +
                "    \"e\": {\"type\":\"Float\", \"value\":5.0}" +
                "  }" +
                "}";

        String msg = "{\"a\":[{\"b\":5.0}], \"c\":\"wwwVwww\"}";
        String config = "{\"d\":\"w\"}";

        assertTrue( engine.evaluateCondition(condition,msg,config) );
    }

}
