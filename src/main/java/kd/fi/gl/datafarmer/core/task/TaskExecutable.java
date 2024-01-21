package kd.fi.gl.datafarmer.core.task;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * Description: taskConfig中，taskParam的反序列化对象，或是原始对象的拆分任务
 *
 * @author ysj
 * @date 2024/1/19
 */
public interface TaskExecutable extends Serializable {

    /**
     * 是否支持拆分
     *
     * @return true -> 需要对参数组进行拆分成子任务执行
     */
    default boolean supportSplit() {
        return false;
    }

    /**
     * 对任务进行拆分
     *
     * @return 拆分后的新任务
     */
    default List<? extends TaskExecutable> split() {
        return Collections.emptyList();
    }

    /**
     * 实际的执行方法
     */
    default void execute() {

    }
}
