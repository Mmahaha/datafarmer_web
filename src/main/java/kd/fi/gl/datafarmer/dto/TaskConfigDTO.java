package kd.fi.gl.datafarmer.dto;

import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.task.enums.TaskStatus;
import kd.fi.gl.datafarmer.core.task.enums.TaskType;
import kd.fi.gl.datafarmer.model.TaskConfig;
import lombok.Data;

/**
 * Description: 前后端交互对象，主要是处理了数据库中taskParam字段的反序列化
 * {@link TaskConfig}
 *
 * @author ysj
 * @date 2024/1/19
 */
@Data
public class TaskConfigDTO<T extends TaskExecutable> {

    private long id;
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


}
