package kd.fi.gl.datafarmer.dao;

import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.core.task.enums.TaskStatus;
import kd.fi.gl.datafarmer.core.task.enums.TaskType;
import kd.fi.gl.datafarmer.model.TaskConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Description:任务的数据库操作类
 *
 * @author ysj
 * @date 2024/1/14
 */

@Repository
public class TaskDao {

    @Autowired
    private JdbcTemplateContainer container;

    public List<TaskConfig> findAll() {
        return null;
    }

    public List<TaskConfig> findByTaskType(TaskType taskType) {
        return null;
    }

    public List<TaskConfig> findByTaskStatus(TaskStatus taskStatus) {
        return null;
    }

    public List<TaskConfig> findByTaskTypeAndStatus(TaskType taskType, TaskStatus taskStatue) {
        return null;
    }

    public TaskConfig findById(Long taskId) {
        return null;
    }

    public void createTask(TaskConfig taskConfig) {

    }

    public void updateTask(Long taskId, TaskConfig updatedTaskConfig) {

    }

    public void deleteTask(Long taskId) {

    }
}
