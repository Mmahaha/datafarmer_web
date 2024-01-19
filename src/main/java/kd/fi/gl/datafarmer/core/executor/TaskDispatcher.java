package kd.fi.gl.datafarmer.core.executor;

import kd.fi.gl.datafarmer.core.util.ProgressRecorder;
import kd.fi.gl.datafarmer.mapper.TaskConfigMapper;
import kd.fi.gl.datafarmer.service.TaskService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@Slf4j
@Component
public class TaskDispatcher {

    @Autowired
    private TaskService taskService;

    @Autowired
    private TaskConfigMapper configMapper;

    public void dispatch() {

    }


    private Callable<?> wrapTask(Callable<?> callable, ProgressRecorder progressRecorder) {
        return () -> {
            long start = System.currentTimeMillis();
            Object result = callable.call();
            long end = System.currentTimeMillis();
            progressRecorder.incrementAndLog(TimeUnit.MILLISECONDS.toSeconds(end - start));
            return result;
        };
    }

}
