package kd.fi.gl.datafarmer.mapper;

import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.common.util.JsonUtils;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import kd.fi.gl.datafarmer.model.TaskConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Description: dto mapper
 *
 * @author ysj
 * @date 2024/1/17
 */
@Component
@RequiredArgsConstructor
public class TaskConfigMapper {

    private final JdbcTemplateContainer container;

    public TaskConfigDTO<? extends TaskExecutable> toDTO(TaskConfig taskConfig) {
        Class<? extends TaskExecutable> paramClass = taskConfig.getTaskType().getParamClass();
        TaskConfigDTO<TaskExecutable> taskConfigDTO = new TaskConfigDTO<>();
        taskConfigDTO.setTaskType(taskConfig.getTaskType());
        taskConfigDTO.setTaskStatus(taskConfig.getTaskStatus());
        taskConfigDTO.setMessage(taskConfig.getMessage());
        taskConfigDTO.setTaskParam(JsonUtils.fromJson(taskConfig.getTaskParam(), paramClass));
        taskConfigDTO.setId(taskConfig.getId());
        return taskConfigDTO;
    }

    public TaskConfig toEntity(TaskConfigDTO<? extends TaskExecutable> taskConfigDTO) {
        TaskConfig taskConfig = new TaskConfig();
        taskConfig.setTaskType(taskConfigDTO.getTaskType());
        taskConfig.setTaskStatus(taskConfigDTO.getTaskStatus());
        taskConfig.setTaskParam(JsonUtils.toJson(taskConfigDTO.getTaskParam()));
        taskConfig.setMessage(taskConfigDTO.getMessage());
        return taskConfig;
    }

}
