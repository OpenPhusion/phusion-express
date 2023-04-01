package cloud.phusion.express.component.scheduling;

import cloud.phusion.Context;
import cloud.phusion.EngineFactory;
import cloud.phusion.PhusionException;
import cloud.phusion.ScheduledTask;
import cloud.phusion.express.util.TimeMarker;
import cloud.phusion.storage.KVStorage;

import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * It is not thread-safe. To be optimized.
 *
 * Execute process: Quartz -> QuartzJobWrapper.run -> TaskManager.run -> ScheduledTask.run
 */
public class TaskManager {
    private static final String _position = TaskManager.class.getName();

    private static Map<String, ScheduledTask> tasks = new ConcurrentHashMap<String, ScheduledTask>();
    private static Map<String, Context> tasksContext = new ConcurrentHashMap<String, Context>();
    private static Map<String, Boolean> tasksClustered = new ConcurrentHashMap<String, Boolean>();
    private static Random random = new Random();

    static {
        random.setSeed(System.currentTimeMillis());
    }

    private static boolean clustered = false;
    private static int clusterRandomRange = 1000;
    private static long clusterLocktime = 10000;

    public static void init(Properties props) {
        if (props != null) {
            clustered = Boolean.parseBoolean(props.getProperty(EngineFactory.Scheduler_Clustered, "false"));
            clusterRandomRange = Integer.parseInt(props.getProperty(EngineFactory.Scheduler_RandomRange, "1000"));
            clusterLocktime = Integer.parseInt(props.getProperty(EngineFactory.Scheduler_LockTime, "10000"));
        }
    }

    public static void addTask(String taskId, ScheduledTask task, boolean clustered, Context ctx) throws Exception {
        if (tasks.containsKey(taskId))
            throw new PhusionException("TASK_EXIST", "Failed to register task", ctx);

        tasks.put(taskId, task);
        tasksContext.put(taskId, ctx);
        tasksClustered.put(taskId, clustered);

        ctx.logInfo(_position, "Task registered");
    }

    public static void removeTask(String taskId, Context ctx) throws Exception {
        if (! tasks.containsKey(taskId))
            throw new PhusionException("TASK_NONE", "Failed to unregister task", ctx);

        tasks.remove(taskId);
        tasksContext.remove(taskId);
        tasksClustered.remove(taskId);

        ctx.logInfo(_position, "Task unregistered");
    }

    public static void clearAllTasks(Context ctx) throws Exception {
        tasks.clear();
        tasksContext.clear();
        tasksClustered.clear();

        ctx.logInfo(_position, "All tasks unregistered");
    }

    public static boolean doesTaskExist(String taskId) {
        return tasks.containsKey(taskId);
    }

    public static void runTask(String taskId) throws Exception {
        if (! tasks.containsKey(taskId))
            throw new PhusionException("TASK_NONE", "Failed to run task", "taskId="+taskId);

        ScheduledTask task = tasks.get(taskId);
        Context c = tasksContext.get(taskId);
        Boolean taskClustered = tasksClustered.get(taskId);

        Context ctx = EngineFactory.createContext(c.getEngine()); // Create new context
        ctx.setContextInfo("taskId", taskId);

        if (clustered && taskClustered) {
            // For load balancing, wait a random period of time to select a node in the cluser
            long msWait = TaskManager.random.nextInt(clusterRandomRange);
            Thread.sleep(msWait);

            // If obtained the lock, run the task, otherwise do not run
            KVStorage storage = ctx.getEngine().getKVStorageForApplication("PhusionLock");
            if (storage.lock(taskId, clusterLocktime)) {
                ctx.logInfo(_position, "Running task in cluster mode", "msWait="+msWait+"ms");
                TimeMarker marker = new TimeMarker();

                try {
                    task.run(taskId, ctx);
                } catch (Exception ex) {
                    throw new PhusionException("TASK_OP", "Failed to run task", ctx, ex);
                } finally {
//                    storage.unlock(taskId); // To block others in the cluster to rerun the task, do not release the lock
                }

                double ms = marker.mark();
                ctx.logInfo(_position, "Task runned", String.format("time=%.1fms", ms));
            }
            else ctx.logInfo(_position, "Task cancelled because no lock available", "msWait="+msWait+"ms");
        }
        else {
            ctx.logInfo(_position, "Running task in stand-alone mode");
            TimeMarker marker = new TimeMarker();

            try {
                task.run(taskId, ctx);
            } catch (Exception ex) {
                throw new PhusionException("TASK_OP", "Failed to run task", ctx, ex);
            }

            double ms = marker.mark();
            ctx.logInfo(_position, "Task runned", String.format("time=%.1fms", ms));
        }
    }

}
