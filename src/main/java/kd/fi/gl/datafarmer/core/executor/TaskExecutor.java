package kd.fi.gl.datafarmer.core.executor;

import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/19
 */

@Component
public class TaskExecutor {

    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;

    public void asyncExecute(List<TaskConfigDTO<? extends TaskExecutable>> taskConfigDTOS) {
        // 使用CompletableFuture实现并行执行
        CompletableFuture<Void>[] futures = taskConfigDTOS.stream()
                .collect(
                        CompletableFuture[]::new,
                        (array, taskConfigDTO) ->
                                array[taskConfigDTO.getTaskType().getOrder() - 1] = CompletableFuture.runAsync(() ->
                                                executeTask(taskConfigDTO),
                                        taskExecutor),
                        CompletableFuture[]::allOf
                );

        // 等待所有任务执行完毕
        CompletableFuture.allOf(futures).join();
    }



}
