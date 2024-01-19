package kd.fi.gl.datafarmer.service;

import kd.fi.gl.datafarmer.core.task.TaskParam;
import kd.fi.gl.datafarmer.core.task.enums.TaskStatus;
import kd.fi.gl.datafarmer.core.task.enums.TaskType;
import kd.fi.gl.datafarmer.dao.TaskDao;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import kd.fi.gl.datafarmer.mapper.TaskConfigMapper;
import kd.fi.gl.datafarmer.model.TaskConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Description:任务操作服务类
 *
 * @author ysj
 * @date 2024/1/14
 */

@Service
public class TaskService {

    @Autowired
    private TaskDao taskDao;

    @Autowired
    private TaskConfigMapper taskConfigMapper;

    public List<TaskConfigDTO<TaskParam>> getAllTasks() {
        List<TaskConfig> taskConfigs = taskDao.findAll();
        return taskConfigs.stream().map(taskConfigMapper::toDTO).collect(Collectors.toList());
    }

    public List<TaskConfigDTO<TaskParam>> getTasksByTaskTypeAndStatus(TaskType taskType, TaskStatus taskStatus) {
        List<TaskConfig> taskConfigs = taskDao.findByTaskTypeAndStatus(taskType, taskStatus);
        return taskConfigs.stream().map(taskConfigMapper::toDTO).collect(Collectors.toList());
    }


    public TaskConfigDTO<TaskParam> getTaskById(Long taskId) {
        TaskConfig taskConfig = taskDao.findById(taskId);
        return taskConfigMapper.toDTO(taskConfig);
    }

    public void createTask(TaskConfigDTO<TaskParam> taskConfigDTO) {
        TaskConfig taskConfig = taskConfigMapper.toEntity(taskConfigDTO);
        taskDao.createTask(taskConfig);
    }

    public void updateTask(Long taskId, TaskConfigDTO<TaskParam> updatedTaskConfigDTO) {
        TaskConfig taskConfig = taskConfigMapper.toEntity(updatedTaskConfigDTO);
        taskDao.updateTask(taskId, taskConfig);
    }

    public void deleteTask(Long taskId) {
        taskDao.deleteTask(taskId);
    }
}
