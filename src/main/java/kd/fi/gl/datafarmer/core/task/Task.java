package kd.fi.gl.datafarmer.core.task;

import org.springframework.scheduling.annotation.Async;

import java.util.concurrent.CompletableFuture;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/19
 */
public interface Task<P extends TaskParam> {

    @Async
    CompletableFuture<Boolean> execute(P param);

}
