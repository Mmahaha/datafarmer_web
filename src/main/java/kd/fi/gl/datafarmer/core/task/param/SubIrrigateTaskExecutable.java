package kd.fi.gl.datafarmer.core.task.param;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

import java.util.List;
import java.util.Set;

/**
 * Description: 灌数任务的子任务，按组织+期间拆分
 *
 * @author ysj
 * @date 2024/1/21
 */

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
public class SubIrrigateTaskExecutable extends IrrigateTaskExecutable {

    private long orgId;
    private long periodId;
    private List<Long> accountIds;
    private List<Long> assgrpIds;
    private List<Long> mainCFItemIds;
    private List<Long> suppCFItemIds;

    @Override
    public boolean supportSplit() {
        return false;
    }

    @Override
    public void execute() {

    }
}
