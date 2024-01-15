package kd.fi.gl.datafarmer.controller;

import kd.fi.gl.datafarmer.model.TaskConfig;
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

    // 查询所有任务
    @GetMapping
    public List<TaskConfig> getAllTasks() {
        return taskService.getAllTasks();
    }

    // 查询单个任务
    @GetMapping("/{taskId}")
    public TaskConfig getTaskById(@PathVariable Long taskId) {
        return taskService.getTaskById(taskId);
    }

    // 创建任务
    @PostMapping
    public TaskConfig createTask(@RequestBody TaskConfig taskConfig) {
        return taskService.createTask(taskConfig);
    }

    // 更新任务
    @PutMapping("/{taskId}")
    public TaskConfig updateTask(@PathVariable Long taskId, @RequestBody TaskConfig updatedTaskConfig) {
        return taskService.updateTask(taskId, updatedTaskConfig);
    }

    // 删除任务
    @DeleteMapping("/{taskId}")
    public void deleteTask(@PathVariable Long taskId) {
        taskService.deleteTask(taskId);
    }
}