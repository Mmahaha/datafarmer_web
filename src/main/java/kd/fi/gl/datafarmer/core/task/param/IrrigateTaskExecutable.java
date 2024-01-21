package kd.fi.gl.datafarmer.core.task.param;

import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.util.DB;
import lombok.Data;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Description: 灌数参数，一个任务组一个参数，可拆分成子参数任务
 *
 * @author ysj
 * @date 2024/1/19
 */
@Data
@Slf4j
@SuperBuilder
public class IrrigateTaskExecutable implements TaskExecutable {

    protected long entryCurrencyId;
    protected int entryRatio;
    protected int repetition;
    protected boolean containsVoucher;
    protected boolean containsBalance;
    protected boolean containsSumBalance;
    protected boolean containsVoucherCount;
    protected boolean containsCashFlow;
    private String orgSelectSql;
    private String startPeriodNumber;
    private String endPeriodNumber;
    private String accountSelectSql;
    private String assgrpSelectSql;
    private String mainCFItemSelectSql;
    private String suppCFItemSelectSql;

    @Override
    public boolean supportSplit() {
        return true;
    }

    @Override
    public void execute() {
        throw new IllegalStateException("can not be executed!");
    }

    /**
     * 执行sql查询，同时按组织+期间拆分成多个子任务
     */
    @Override
    public List<SubIrrigateTaskExecutable> split() {
        JdbcTemplate sysJdbcTemplate = DB.getSysJdbcTemplate();
        JdbcTemplate fiJdbcTemplate = DB.getFiJdbcTemplate();
        List<Long> orgIds = sysJdbcTemplate.queryForList(orgSelectSql, Long.class);
        List<Long> periodIds = fiJdbcTemplate.queryForList("select fid from t_bd_period where fnumber >= ? and" +
                        " fnumber <= ? and ftypeid = ? and fisadjustperiod = ?", Long.class,
                startPeriodNumber, endPeriodNumber, 1, '0');
        List<Long> accountIds = fiJdbcTemplate.queryForList(accountSelectSql, Long.class);
        List<Long> assgrpIds = fiJdbcTemplate.queryForList(assgrpSelectSql, Long.class);
        List<Long> mainCFItemIds = containsCashFlow ? fiJdbcTemplate.queryForList(mainCFItemSelectSql, Long.class) : Collections.emptyList();
        List<Long> suppCFItemIds = containsCashFlow ? fiJdbcTemplate.queryForList(suppCFItemSelectSql, Long.class) : Collections.emptyList();
        List<SubIrrigateTaskExecutable> result = new ArrayList<>(orgIds.size() * periodIds.size());
        for (Long orgId : orgIds) {
            for (Long periodId : periodIds) {
                result.add(SubIrrigateTaskExecutable.builder()
                        .entryCurrencyId(entryCurrencyId)
                        .entryRatio(entryRatio)
                        .repetition(repetition)
                        .containsVoucher(containsVoucher)
                        .containsBalance(containsBalance)
                        .containsSumBalance(containsSumBalance)
                        .containsVoucherCount(containsVoucherCount)
                        .containsCashFlow(containsCashFlow)
                        .orgId(orgId)
                        .periodId(periodId)
                        .accountIds(accountIds)
                        // todo split
                        .assgrpIds(assgrpIds)
                        .mainCFItemIds(mainCFItemIds)
                        .suppCFItemIds(suppCFItemIds)
                        .build());
            }
        }
        return result;
    }

}
