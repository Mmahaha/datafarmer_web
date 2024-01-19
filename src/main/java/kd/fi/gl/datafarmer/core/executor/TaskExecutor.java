package kd.fi.gl.datafarmer.core.executor;

import kd.fi.gl.datafarmer.core.task.TaskParam;
import kd.fi.gl.datafarmer.dto.TaskConfigDTO;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/19
 */

@Component
public class TaskExecutor {

    public void asyncExecute(List<TaskConfigDTO<TaskParam>> taskConfigDTOS) {
        Collections.sort();

    }

}
