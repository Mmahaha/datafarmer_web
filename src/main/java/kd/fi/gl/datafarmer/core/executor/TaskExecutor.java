package kd.fi.gl.datafarmer.core.executor;

import kd.fi.gl.datafarmer.common.exception.impl.ParseConfigException;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.util.ProgressRecorder;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import kd.fi.gl.datafarmer.service.TaskService;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/19
 */

@Component
//@RequiredArgsConstructor
@Slf4j
public class TaskExecutor {

    @Resource(name = "dataFarmerTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    private final TaskService taskService;

    @Autowired
    public TaskExecutor(@Lazy TaskService taskService) {
        this.taskService = taskService;
    }


    @Async("defaultExecutor")
    public void asyncExecute(List<TaskConfigDTO<? extends TaskExecutable>> taskConfigDTOS) {
        try {
            executeTasks(taskConfigDTOS, 1); // Start executing tasks with order 1
            log.info("所有任务已执行完成~");
        } catch (Exception e) {
            // Handle exception
            log.error("execute failed", e);
            throw e;
        }
    }

    @SneakyThrows
    private void executeTasks(List<TaskConfigDTO<? extends TaskExecutable>> taskConfigDTOS, int order) {
        if (order <= getMaxOrder(taskConfigDTOS)) {
            List<TaskConfigDTO<? extends TaskExecutable>> orderTaskConfigs = getTasksByOrder(taskConfigDTOS, order);
            Map<TaskConfigDTO<? extends TaskExecutable>, List<? extends TaskExecutable>> configSplitTasksMap =
                    orderTaskConfigs.parallelStream().collect(Collectors.toMap(
                            config -> config, config -> splitTasks(config).collect(Collectors.toList())));
            List<CompletableFuture<Void>> curOrderTaskFuture = new ArrayList<>(configSplitTasksMap.size());
            configSplitTasksMap.forEach((taskConfigDTO, taskExecutables) -> {
                // 每个任务组一个进度器
                ProgressRecorder progressRecorder = new ProgressRecorder(taskConfigDTO.getId(), taskExecutables.size(), taskService);
                taskService.setTaskInRunning(taskConfigDTO.getId());
                curOrderTaskFuture.add(CompletableFuture
                        .allOf(taskExecutables.stream()
                                .map(task -> CompletableFuture.runAsync(wrapRunningTask(() -> task.execute(), progressRecorder), taskExecutor))
                                .toArray(CompletableFuture[]::new)
                        ).whenComplete((r,t) -> {
                            if (t == null) {
                                taskService.setTaskFinished(taskConfigDTO.getId());
                            } else {
                                taskService.setTaskError(taskConfigDTO.getId(), t.getClass().getName() + ":" + t.getMessage());
                            }
                        }));
            });
            CompletableFuture.allOf(curOrderTaskFuture.toArray(new CompletableFuture[0])).whenComplete((r, t) -> {
                if (t != null) {
                    log.error("任务执行出现异常：", t);
                } else {
                    executeTasks(taskConfigDTOS, order + 1);
                }
            }).get();
        }
    }

    private Runnable wrapRunningTask(Runnable runnable, ProgressRecorder progressRecorder) {
        return () -> {
            long start = System.currentTimeMillis();
            runnable.run();
            long end = System.currentTimeMillis();
            progressRecorder.incrementAndLog(TimeUnit.MILLISECONDS.toSeconds(end - start));
        };
    }


    private int getMaxOrder(List<TaskConfigDTO<? extends TaskExecutable>> taskConfigDTOS) {
        return taskConfigDTOS.stream()
                .mapToInt(taskConfigDTO -> taskConfigDTO.getTaskType().getOrder())
                .max()
                .orElse(0);
    }

    private List<TaskConfigDTO<? extends TaskExecutable>> getTasksByOrder(List<TaskConfigDTO<? extends TaskExecutable>> taskConfigDTOS, int order) {
        return taskConfigDTOS.stream()
                .filter(taskConfigDTO -> taskConfigDTO.getTaskType().getOrder() == order)
                .collect(Collectors.toList());
    }

    private Stream<? extends TaskExecutable> splitTasks(TaskConfigDTO<? extends TaskExecutable> taskConfigDTO) {
        try {
            TaskExecutable taskParam = taskConfigDTO.getTaskParam();
            return taskParam.supportSplit() ? taskParam.split().stream() : Stream.of(taskParam);
        } catch (Exception e) {
            log.error("配置解析异常，taskId=" + taskConfigDTO.getId() + ",exception=" + e.getMessage(), e);
            throw new ParseConfigException("taskId=" + taskConfigDTO.getId() + ",exception=" + e.getMessage(), e);
        }
    }
}

