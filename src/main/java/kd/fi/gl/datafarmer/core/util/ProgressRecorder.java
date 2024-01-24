package kd.fi.gl.datafarmer.core.util;

import kd.fi.gl.datafarmer.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class ProgressRecorder {

    private final Long taskId;
    private final int total;
    private final TaskService taskService;
    private final int reportFrequency;

    private final AtomicInteger progress = new AtomicInteger(0);

    public ProgressRecorder(Long taskId, int total, TaskService taskService) {
        this.taskId = taskId;
        this.total = total;
        this.taskService = taskService;
        reportFrequency = total / 5000 + 1;    // 每个任务组最多通过db上报的次数
    }

    public void incrementAndLog(long seconds) {
        int curProgress = progress.incrementAndGet();
        log.info("taskId:{} progress:{}/{}, cost {} s", taskId, curProgress, total, seconds);
        if (curProgress % reportFrequency == 0) {
            taskService.updateTaskMessage(taskId, String.format("%s/%s", curProgress, total));
        }
    }

}
