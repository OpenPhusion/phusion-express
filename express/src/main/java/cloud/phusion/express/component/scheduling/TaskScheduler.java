package cloud.phusion.express.component.scheduling;

import cloud.phusion.Context;
import cloud.phusion.EngineFactory;
import cloud.phusion.PhusionException;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import static org.quartz.JobBuilder.*;
import static org.quartz.SimpleScheduleBuilder.simpleSchedule;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.JobKey.*;
import static org.quartz.CronScheduleBuilder.*;

import java.util.Date;
import java.util.Properties;

/**
 * Not use the cluster feature of quartz, use simple Redis lock instead. To be optimized.
 *
 * quartz.properties:
 * https://www.quartz-scheduler.org/documentation/quartz-2.3.0/configuration/
 */
public class TaskScheduler {
    private static final String _position = TaskScheduler.class.getName();

    private String jobGroup = "phusion";
    private Scheduler scheduler = null;

    public TaskScheduler(Properties props) throws Exception {
        super();

        if (props == null) {
            this.scheduler = StdSchedulerFactory.getDefaultScheduler();
        }
        else {
            StdSchedulerFactory factory = new StdSchedulerFactory();
            Properties qprops = new Properties();

            int threadCount = Integer.parseInt(props.getProperty(EngineFactory.Scheduler_ThreadCount, "10"));
            qprops.setProperty("org.quartz.threadPool.threadCount", ""+threadCount);

            factory.initialize(qprops);
            this.scheduler = factory.getScheduler();

//            this.jobGroup = this.scheduler.getSchedulerInstanceId();
        }
    }

    public TaskScheduler() throws Exception {
        this(null);
    }

    public void start() throws Exception {
        scheduler.start();
    }

    public void stop() throws Exception {
        // scheduler.shutdown(true);
        scheduler.standby();
    }

    public void scheduleTask(String taskId, String cron, Context ctx) throws Exception {
        ctx.logInfo(_position, "Scheduling CRON task", "cron="+cron);

        JobDetail job = newJob(QuartzJobWrapper.class)
                .withIdentity(taskId, jobGroup)
                .build();

        Trigger trigger = newTrigger()
                .withIdentity(taskId, jobGroup)
                .withSchedule(cronSchedule(cron))
                .build();

        try {
            scheduler.scheduleJob(job, trigger);

            ctx.logInfo(_position, "CRON task scheduled");
        } catch (Exception ex) {
            throw new PhusionException("TASK_OP_SCH", "Failed to schedule CRON task", ctx, ex);
        }
    }

    public void scheduleTask(String taskId, Date startTime, long intervalInSeconds, long repeatCount, Context ctx) throws Exception {
        ctx.logInfo(_position, "Scheduling periodic task", String.format("startTime=%tF %tT, intervalInSeconds=%d, repeatCount=%d",
                startTime, startTime, intervalInSeconds, repeatCount));

        JobDetail job = newJob(QuartzJobWrapper.class)
                .withIdentity(taskId, jobGroup)
                .build();

        TriggerBuilder<Trigger> tb = newTrigger().withIdentity(taskId, jobGroup);

        if (startTime == null)
            tb.startNow();
        else
            tb.startAt(startTime);

        if (repeatCount == 0)
            tb.withSchedule(simpleSchedule()
                    .withIntervalInSeconds((int)intervalInSeconds)
                    .repeatForever());
        else
            tb.withSchedule(simpleSchedule()
                    .withIntervalInSeconds((int)intervalInSeconds)
                    .withRepeatCount((int)(repeatCount-1))); // repeatCount-1: ignore the first run

        Trigger trigger = tb.build();

        try {
            scheduler.scheduleJob(job, trigger);

            ctx.logInfo(_position, "Periodic task scheduled");
        } catch (Exception ex) {
            throw new PhusionException("TASK_OP_SCH", "Failed to schedule periodic task", ctx, ex);
        }
    }

    public void removeScheduledTask(String taskId, Context ctx) throws Exception {
        try {
            scheduler.deleteJob(jobKey(taskId, jobGroup));

            ctx.logInfo(_position, "Task removed");
        } catch (Exception ex) {
            throw new PhusionException("TASK_OP_SCH", "Failed to remove task", ctx, ex);
        }
    }

    public void clearAllTasks(Context ctx) throws Exception {
        try {
            scheduler.clear();

            ctx.logInfo(_position, "All tasks cleared");
        } catch (Exception ex) {
            throw new PhusionException("TASK_OP_SCH", "Failed to clear tasks", ctx, ex);
        }
    }

}
