package kd.fi.gl.datafarmer.service;

import kd.fi.gl.datafarmer.core.executor.TaskExecutor;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.task.enums.TaskStatus;
import kd.fi.gl.datafarmer.core.task.enums.TaskType;
import kd.fi.gl.datafarmer.core.task.impl.IrrigateTaskExecutable;
import kd.fi.gl.datafarmer.dao.TaskDao;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import kd.fi.gl.datafarmer.mapper.TaskConfigMapper;
import kd.fi.gl.datafarmer.model.TaskConfig;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Description:任务操作服务类
 *
 * @author ysj
 * @date 2024/1/14
 */

@Service
@RequiredArgsConstructor
@SuppressWarnings("unchecked")
public class TaskService {

    private final TaskDao taskDao;

    private final TaskExecutor taskExecutor;

    private final TaskConfigMapper taskConfigMapper;


    public List<TaskConfigDTO<? extends TaskExecutable>> getAllReadyTasks() {
        List<TaskConfig> taskConfigs = taskDao.findByTaskStatus(TaskStatus.READY);
        return taskConfigs.stream().map(taskConfigMapper::toDTO).collect(Collectors.toList());
    }

    public List<TaskConfigDTO<? extends TaskExecutable>> getAllTasks() {
        List<TaskConfig> taskConfigs = taskDao.findAll();
        return taskConfigs.stream().map(taskConfigMapper::toDTO).collect(Collectors.toList());
    }

    public List<TaskConfigDTO<IrrigateTaskExecutable>> getAllIrrigateTasks() {
        List<TaskConfig> taskConfigs = taskDao.findByTaskType(TaskType.IRRIGATE);
        return taskConfigs.stream()
                .map(taskConfig -> (TaskConfigDTO<IrrigateTaskExecutable>) taskConfigMapper.toDTO(taskConfig))
                .collect(Collectors.toList());
    }

    public List<TaskConfigDTO<? extends TaskExecutable>> getTasksByTaskTypeAndStatus(TaskType taskType, TaskStatus taskStatus) {
        List<TaskConfig> taskConfigs = taskDao.findByTaskTypeAndStatus(taskType, taskStatus);
        return taskConfigs.stream().map(taskConfigMapper::toDTO).collect(Collectors.toList());
    }


    public TaskConfigDTO<? extends TaskExecutable> getTaskById(Long taskId) {
        TaskConfig taskConfig = taskDao.findById(taskId);
        return taskConfigMapper.toDTO(taskConfig);
    }

    public void createTask(TaskConfigDTO<? extends TaskExecutable> taskConfigDTO) {
        TaskConfig taskConfig = taskConfigMapper.toEntity(taskConfigDTO);
        taskDao.createTask(taskConfig);
    }

    public void updateTask(Long taskId, TaskConfigDTO<? extends TaskExecutable> updatedTaskConfigDTO) {
        TaskConfig taskConfig = taskConfigMapper.toEntity(updatedTaskConfigDTO);
        taskDao.updateTask(taskId, taskConfig);
    }

    public void deleteTask(Long taskId) {
        taskDao.deleteTask(taskId);
    }

    public void executeAll() {
        List<TaskConfigDTO<? extends TaskExecutable>> allTasks = getAllReadyTasks();
        taskExecutor.asyncExecute(allTasks);
    }

    public List<String> getAllTaskTypes() {
        return Arrays.stream(TaskType.values()).map(Enum::name).collect(Collectors.toList());
    }
}
