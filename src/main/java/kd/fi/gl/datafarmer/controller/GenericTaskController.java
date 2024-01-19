package kd.fi.gl.datafarmer.controller;

import kd.fi.gl.datafarmer.common.ApiResponse;
import kd.fi.gl.datafarmer.core.task.TaskParam;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;

import java.util.List;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/19
 */
public interface GenericTaskController<T extends TaskParam> {
    ApiResponse<List<TaskConfigDTO<T>>> getAllTasks();

    ApiResponse<TaskConfigDTO<T>> getTaskById(Long taskId);

    ApiResponse<Boolean> createTask(TaskConfigDTO<T> taskConfig);

    ApiResponse<Boolean> updateTask(Long taskId, TaskConfigDTO<T> updatedTaskConfig);

    ApiResponse<Boolean> deleteTask(Long taskId);
}
