package kd.fi.gl.datafarmer.core.task.enums;

import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.task.impl.IrrigateTaskExecutable;
import kd.fi.gl.datafarmer.core.task.impl.RebuildIndexTaskExecutable;

/**
 * Description: 枚举所有的任务类型
 *
 * @author ysj
 * @date 2024/1/19
 */
public enum TaskType {
    /**
     * 数据灌输任务
     */
    IRRIGATE(IrrigateTaskExecutable.class, 1),
    /**
     * 索引重建任务
     */
    REBUILD_INDEX(RebuildIndexTaskExecutable.class, 2);

    /**
     * 该任务类型对应的参数类，每一个任务类型都应当有一个自己的参数
     */
    private final Class<? extends TaskExecutable> paramClass;
    /**
     * 任务类型的执行顺序，通常来说一批含有不同任务类型的任务一起执行时，任务是有先后顺序的。
     * 不同order的任务类型无法并行执行
     * 当两种任务的order一致时，则可以并行执行以提升效率
     */
    private final int order;

    TaskType(Class<? extends TaskExecutable> paramClass, int order) {
        this.paramClass = paramClass;
        this.order = order;
    }

    public Class<? extends TaskExecutable> getParamClass() {
        return paramClass;
    }

    public int getOrder() {
        return order;
    }
}
