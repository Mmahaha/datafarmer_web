package kd.fi.gl.datafarmer.dao;

import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.core.task.enums.TaskStatus;
import kd.fi.gl.datafarmer.core.task.enums.TaskType;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.model.TaskConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Description:任务的数据库操作类
 *
 * @author ysj
 * @date 2024/1/14
 */

@Repository
@RequiredArgsConstructor
public class TaskDao {

    private final JdbcTemplateContainer container;

    public List<TaskConfig> findAll() {
        List<Map<String, Object>> rows = container.getFi()
                .queryForList("select * from datafarmer_task");
        return mapRowsToEntities(rows);
    }

    public List<TaskConfig> findByTaskType(TaskType taskType) {
        List<Map<String, Object>> rows = container.getFi()
                .queryForList("select * from datafarmer_task where ftasktype = ?", taskType.name());
        return mapRowsToEntities(rows);
    }

    public List<TaskConfig> findByTaskStatus(TaskStatus taskStatus) {
        List<Map<String, Object>> rows = container.getFi()
                .queryForList("select * from datafarmer_task where ftaskstatus = ?", taskStatus.name());
        return mapRowsToEntities(rows);
    }

    public List<TaskConfig> findByTaskTypeAndStatus(TaskType taskType, TaskStatus taskStatue) {
        return null;
    }

    public TaskConfig findById(Long taskId) {
        return null;
    }

    public void createTask(TaskConfig taskConfig) {
        container.getFi().update("insert into datafarmer_task(ftasktype, ftaskparam, fmessage, ftaskstatus) values(?,?,?,?)",
                taskConfig.getTaskType().name(), taskConfig.getTaskParam(), taskConfig.getMessage(), TaskStatus.READY.name());
    }

    public void updateTask(Long taskId, TaskConfig updatedTaskConfig) {

    }

    public void deleteTask(Long taskId) {

    }

    public void updateTaskStatus(Long taskId, TaskStatus taskStatus) {
        container.getFi()
                .update("update datafarmer_task set ftaskstatus = ? where fid = ?", taskStatus.name(), taskId);
    }

    public void updateTaskMessage(Long taskId, String message) {
        container.getFi()
                .update("update datafarmer_task set fmessage = ? where fid = ?", message, taskId);
    }

    private List<TaskConfig> mapRowsToEntities(List<Map<String,Object>> rows) {
        return rows.stream().map(row ->
            TaskConfig.builder()
                    .id(Long.parseLong(row.get("fid").toString()))
                    .taskType(TaskType.valueOf((String) row.get("ftasktype")))
                    .taskParam((String) row.get("ftaskparam"))
                    .message((String) row.get("fmessage"))
                    .taskStatus(TaskStatus.valueOf((String) row.get("ftaskstatus")))
                    .build()
        ).collect(Collectors.toList());
    }
}
