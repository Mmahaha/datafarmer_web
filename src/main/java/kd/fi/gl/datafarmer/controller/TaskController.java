package kd.fi.gl.datafarmer.controller;

import kd.fi.gl.datafarmer.common.ApiResponse;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.task.impl.IrrigateTaskExecutable;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import kd.fi.gl.datafarmer.service.TaskService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Description:任务操作的控制器
 *
 * @author ysj
 * @date 2024/1/14
 */

@RestController
@RequestMapping("/api/tasks")
@CrossOrigin(originPatterns = "*")
@RequiredArgsConstructor
public class TaskController {

    private final TaskService taskService;

    // 查询所有任务
    @GetMapping
    public ApiResponse<List<TaskConfigDTO<? extends TaskExecutable>>> getAllTasks() {
        return ApiResponse.success(taskService.getAllTasks());
    }

    // 查询所有任务类型
    @GetMapping("/types")
    public ApiResponse<List<String>> getAllTaskTypes() {
        // 调用 taskService 或者其他服务层方法获取所有任务类型
        List<String> taskTypes = taskService.getAllTaskTypes();
        return ApiResponse.success(taskTypes);
    }


    // 查询所有灌数任务
    @GetMapping("/irrigate")
    public ApiResponse<List<TaskConfigDTO<IrrigateTaskExecutable>>> getAllIrrigateTasks() {
        return ApiResponse.success(taskService.getAllIrrigateTasks());
    }

    @PostMapping("/submit")
    public ApiResponse<Boolean> submit() {
        taskService.executeAll();
        return ApiResponse.success(Boolean.TRUE);
    }

    @GetMapping("/{taskId}")
    public ApiResponse<TaskConfigDTO<? extends TaskExecutable>> getTaskById(@PathVariable Long taskId) {
        return ApiResponse.success(taskService.getTaskById(taskId));
    }


    // 创建任务
    @PostMapping("/irrigate")
    public ApiResponse<Boolean> createIrrigateTask(@RequestBody TaskConfigDTO<IrrigateTaskExecutable> taskConfigDTO) {
        taskService.createTask(taskConfigDTO);
        return ApiResponse.success(Boolean.TRUE);
    }

    // 更新任务
    @PutMapping("/{taskId}")
    public ApiResponse<Boolean> updateTask(@PathVariable Long taskId, @RequestBody TaskConfigDTO<TaskExecutable> updatedTaskConfig) {
        taskService.updateTask(taskId, updatedTaskConfig);
        return ApiResponse.success(Boolean.TRUE);
    }

    // 删除任务
    @DeleteMapping("/{taskId}")
    public ApiResponse<Boolean> deleteTask(@PathVariable Long taskId) {
        taskService.deleteTask(taskId);
        return ApiResponse.success(Boolean.TRUE);
    }
}