package kd.fi.gl.datafarmer.dto;

import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.task.enums.TaskStatus;
import kd.fi.gl.datafarmer.core.task.enums.TaskType;
import kd.fi.gl.datafarmer.model.TaskConfig;

/**
 * Description: 前后端交互对象，主要是处理了数据库中taskParam字段的反序列化
 * {@link TaskConfig}
 *
 * @author ysj
 * @date 2024/1/19
 */
public class TaskConfigDTO<T extends TaskExecutable> {

    private TaskType taskType;
    /**
     * 任务参数
     */
    private T taskParam;
    /**
     * 任务状态
     */
    private TaskStatus taskStatus;
    /**
     * 任务信息，可存放完成情况/错误信息
     */
    private String message;

    public TaskType getTaskType() {
        return taskType;
    }

    public void setTaskType(TaskType taskType) {
        this.taskType = taskType;
    }

    public T getTaskParam() {
        return taskParam;
    }

    public void setTaskParam(T taskParam) {
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
