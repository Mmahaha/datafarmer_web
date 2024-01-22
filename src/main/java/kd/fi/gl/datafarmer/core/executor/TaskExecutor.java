package kd.fi.gl.datafarmer.core.executor;

import kd.fi.gl.datafarmer.common.exception.ParseConfigException;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

    public void asyncExecute(List<TaskConfigDTO<? extends TaskExecutable>> taskConfigDTOS) {
        try {
            executeTasks(taskConfigDTOS, 1); // Start executing tasks with order 1
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
            List<? extends TaskExecutable> splitTaskExecutables = orderTaskConfigs.stream().flatMap(this::splitTasks)
                    .collect(Collectors.toList());
            CompletableFuture<Void> result = CompletableFuture.allOf(splitTaskExecutables.stream()
                    .map(task -> CompletableFuture.runAsync(() -> task.execute(), taskExecutor))
                    .toArray(CompletableFuture[]::new));
            result.whenComplete((r, t) -> {
                if (t != null) {
                    log.error("任务执行出现异常：", t);
                } else {
                    executeTasks(taskConfigDTOS, order + 1);
                }
            }).get();
        }
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

