package kd.fi.gl.datafarmer.controller.impl;

import kd.fi.gl.datafarmer.common.ApiResponse;
import kd.fi.gl.datafarmer.controller.GenericTaskController;
import kd.fi.gl.datafarmer.core.task.param.IrrigateTaskParam;
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
@RequestMapping("/api/tasks/irrigate")
@CrossOrigin(originPatterns = "*")
public class IrrigateTaskController implements GenericTaskController<IrrigateTaskParam> {

    @Autowired
    private TaskService taskService;

    // 查询所有任务
    @GetMapping
    @Autowired
    public ApiResponse<List<TaskConfigDTO<IrrigateTaskParam>>> getAllTasks() {
        return ApiResponse.success(taskService.getAllTasks());
    }

    @Override
    @GetMapping("/{taskId}")
    public ApiResponse<TaskConfigDTO<IrrigateTaskParam>> getTaskById(@PathVariable Long taskId) {
        return ApiResponse.success(taskService.getTaskById(taskId));
    }


    // 创建任务
    @Override
    @PostMapping
    public ApiResponse<Boolean> createTask(@RequestBody TaskConfigDTO<IrrigateTaskParam> taskConfigDTO) {
        taskService.createTask(taskConfigDTO);
        return ApiResponse.success(Boolean.TRUE);
    }

    // 更新任务
    @Override
    @PutMapping("/{taskId}")
    public ApiResponse<Boolean> updateTask(@PathVariable Long taskId, @RequestBody TaskConfigDTO<IrrigateTaskParam> updatedTaskConfig) {
        taskService.updateTask(taskId, updatedTaskConfig);
        return ApiResponse.success(Boolean.TRUE);
    }

    // 删除任务
    @Override
    @DeleteMapping("/{taskId}")
    public ApiResponse<Boolean> deleteTask(@PathVariable Long taskId) {
        taskService.deleteTask(taskId);
        return ApiResponse.success(Boolean.TRUE);
    }
}