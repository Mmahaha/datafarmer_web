package kd.fi.gl.datafarmer.service;

import kd.fi.gl.datafarmer.model.TaskConfig;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Description:任务操作服务类
 *
 * @author ysj
 * @date 2024/1/14
 */

@Service
public class TaskService {
    public List<TaskConfig> getAllTasks() {
        return null;
    }

    public TaskConfig getTaskById(Long taskId) {
        return null;

    }

    public TaskConfig createTask(TaskConfig taskConfig) {
        return null;

    }

    public TaskConfig updateTask(Long taskId, TaskConfig updatedTaskConfig) {
        return null;

    }

    public void deleteTask(Long taskId) {

    }

    public TaskService() {
        System.out.println();
    }
}