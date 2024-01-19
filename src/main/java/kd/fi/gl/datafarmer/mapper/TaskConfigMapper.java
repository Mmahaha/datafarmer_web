package kd.fi.gl.datafarmer.mapper;

import com.alibaba.druid.util.StringUtils;
import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.common.util.JsonUtils;
import kd.fi.gl.datafarmer.core.task.TaskParam;
import kd.fi.gl.datafarmer.core.util.BookService;
import kd.fi.gl.datafarmer.core.util.PeriodVOBuilder;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import kd.fi.gl.datafarmer.dto.TaskConfigGroupDTO;
import kd.fi.gl.datafarmer.model.TaskConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Description: dto mapper
 *
 * @author ysj
 * @date 2024/1/17
 */
@Component
public class TaskConfigMapper {

    @Autowired
    private JdbcTemplateContainer container;

    public TaskConfigDTO<TaskParam> toDTO(TaskConfig taskConfig) {
        Class<? extends TaskParam> paramClass = taskConfig.getTaskType().getParamClass();
        TaskConfigDTO<TaskParam> taskConfigDTO = new TaskConfigDTO<>();
        taskConfigDTO.setTaskType(taskConfig.getTaskType());
        taskConfigDTO.setTaskStatus(taskConfig.getTaskStatus());
        taskConfigDTO.setMessage(taskConfig.getMessage());
        taskConfigDTO.setTaskParam(JsonUtils.fromJson(taskConfig.getTaskParam(), paramClass));
        return taskConfigDTO;
    }

    public TaskConfig toEntity(TaskConfigDTO<TaskParam> taskConfigDTO) {
        TaskConfig taskConfig = new TaskConfig();
        taskConfig.setTaskType(taskConfigDTO.getTaskType());
        taskConfig.setTaskStatus(taskConfigDTO.getTaskStatus());
        taskConfig.setTaskParam(JsonUtils.toJson(taskConfigDTO.getTaskParam()));
        taskConfig.setMessage(taskConfigDTO.getMessage());
        return taskConfig;
    }

}
