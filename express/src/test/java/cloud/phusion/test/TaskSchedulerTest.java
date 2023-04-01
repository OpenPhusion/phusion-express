package cloud.phusion.test;

import cloud.phusion.Context;
import cloud.phusion.Engine;
import cloud.phusion.ScheduledTask;
import org.junit.*;

import java.util.Date;

public class TaskSchedulerTest implements ScheduledTask {
    static Engine engine = null;
    static Context ctx = null;

    private static int count = 0;

    @BeforeClass
    public static void setUp() throws Exception {
//        Properties props = new Properties();
//        props.setProperty(EngineFactory.Scheduler_Enabled, "true");
//        props.setProperty(EngineFactory.Scheduler_Clustered, "true");
//        props.setProperty(EngineFactory.Scheduler_RandomRange, "500");
//        props.setProperty(EngineFactory.Scheduler_LockTime, "2000");
//
//        props.setProperty(EngineFactory.Redis_Host, "192.168.1.1");
//        props.setProperty(EngineFactory.Redis_Port, "1111");
//        props.setProperty(EngineFactory.Redis_Database, "0");
//        props.setProperty(EngineFactory.Redis_Auth, "123456");
//
//        engine = EngineFactory.createEngine(props);
//        ctx = new ExpressContext(engine);
//
//        engine.start(ctx);
    }

    @Test
    public void testCluster() throws Exception {
        // mvn test -Dtest=TaskSchedulerTest#testCluster

//        ScheduledTask task = new TaskSchedulerTest();
//        engine.scheduleTask("SharedTask1", task, "0/3 * * * * ?", ctx);
//
//        Thread.sleep(200000);
    }

    @Test
    public void testStartNow() throws Exception {
//        count = 0;
//        ScheduledTask task = new TaskSchedulerTest();
//
//        engine.scheduleTask("task1", task, 1, ctx);
//        assertTrue(engine.doesTaskExist("task1"));
//
//        Thread.sleep(2500);
//
//        assertEquals(3, count);
//
//        engine.removeScheduledTask("task1", ctx);
//        assertFalse(engine.doesTaskExist("task1"));
    }

    @Test
    public void testStartLater() throws Exception {
//        count = 0;
//        ScheduledTask task = new TaskSchedulerTest();
//
//        Calendar c = Calendar.getInstance();
//        c.setTime(new Date());
//        c.add(Calendar.SECOND, 1);
//
//        engine.scheduleTask("task1", task, c.getTime(), 1, 0, ctx);
//        assertTrue(engine.doesTaskExist("task1"));
//
//        Thread.sleep(1500);
//
//        assertEquals(count, 1);
//
//        engine.removeScheduledTask("task1", ctx);
//        assertFalse(engine.doesTaskExist("task1"));
    }

    @Test
    public void testRunTimes() throws Exception {
//        count = 0;
//        ScheduledTask task = new TaskSchedulerTest();
//
//        engine.scheduleTask("task1", task, 1, 2, ctx);
//        assertTrue(engine.doesTaskExist("task1"));
//
//        Thread.sleep(2900);
//
//        assertEquals(count, 2);
//
//        engine.removeScheduledTask("task1", ctx);
//        assertFalse(engine.doesTaskExist("task1"));
    }

    @Test
    public void testCron() throws Exception {
//        count = 0;
//        ScheduledTask task = new TaskSchedulerTest();
//
//        engine.scheduleTask("task1", task, "0/1 * * * * ?", ctx);
//        assertTrue(engine.doesTaskExist("task1"));
//
//        Thread.sleep(1100);
//
//        assertEquals(count, 1);
//
//        engine.removeScheduledTask("task1", ctx);
//        assertFalse(engine.doesTaskExist("task1"));
    }

    @AfterClass
    public static void tearDown() throws Exception {
//        engine.stop(ctx);
    }

    @Override
    public void run(String taskId, Context ctx) {
        count++;
        String t = (new Date()).toString();
        System.out.println("Running task: "+taskId+" "+count+" times at "+t);
    }

}
