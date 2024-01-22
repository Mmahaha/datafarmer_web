package kd.fi.gl.datafarmer.model;

import kd.fi.gl.datafarmer.core.task.enums.TaskStatus;
import kd.fi.gl.datafarmer.core.task.enums.TaskType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;

/**
 * Description:任务的数据库实体类
 *
 * @author ysj
 * @date 2024/1/14
 */

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TaskConfig {

    private long id;
    /**
     * 任务类型
     */
    private TaskType taskType;
    /**
     * 任务参数
     */
    private String taskParam;
    /**
     * 任务状态
     */
    private TaskStatus taskStatus;
    /**
     * 任务信息，可存放完成情况/错误信息
     */
    private String message;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public String getTaskParam() {
        return taskParam;
    }

    public void setTaskParam(String taskParam) {
        this.taskParam = taskParam;
    }

    public TaskStatus getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(TaskStatus taskStatus) {
        this.taskStatus = taskStatus;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

}

