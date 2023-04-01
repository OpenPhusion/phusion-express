package cloud.phusion.express.component.scheduling;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class QuartzJobWrapper implements Job {

    @Override
    public void execute(JobExecutionContext ctx) throws JobExecutionException {
        String taskId = ctx.getJobDetail().getKey().getName();

        try {
            TaskManager.runTask(taskId);
        } catch (Exception ex) {
            throw new JobExecutionException(ex);
        }
    }

}
