package kd.fi.gl.datafarmer.controller.impl;

import kd.fi.gl.datafarmer.common.ApiResponse;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.task.param.IrrigateTaskExecutable;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import kd.fi.gl.datafarmer.service.TaskService;
import org.springframework.beans.factory.annotation.Autowired;
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
public class TaskController {

    @Autowired
    private TaskService taskService;

    // 查询所有灌数任务
    @GetMapping("/irrigate")
    @Autowired
    public ApiResponse<List<TaskConfigDTO<IrrigateTaskExecutable>>> getAllIrrigateTasks() {
        return ApiResponse.success(taskService.getAllIrrigateTasks());
    }

    @GetMapping("/{taskId}")
    public ApiResponse<TaskConfigDTO<? extends TaskExecutable>> getTaskById(@PathVariable Long taskId) {
        return ApiResponse.success(taskService.getTaskById(taskId));
    }


    // 创建任务
    @PostMapping
    public ApiResponse<Boolean> createTask(@RequestBody TaskConfigDTO<TaskExecutable> taskConfigDTO) {
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