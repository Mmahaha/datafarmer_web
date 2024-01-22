package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.util.AssGrpBuilder;
import kd.fi.gl.datafarmer.core.util.BookService;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.DateUtils;
import kd.fi.gl.datafarmer.core.util.PeriodVOBuilder;
import kd.fi.gl.datafarmer.core.util.RowsBuilder;
import kd.fi.gl.datafarmer.core.util.sharding.VoucherShardingService;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.Assert;

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
@NoArgsConstructor
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
    private int distinctSign;

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
        List<BookService.BookVO> bookVOList = BookService.getBookVOsByOrg(orgIds);
        List<PeriodVOBuilder.PeriodVO> periodVOList = new PeriodVOBuilder(1, startPeriodNumber, endPeriodNumber).getPeriodVOList();
        List<Long> accountIds = fiJdbcTemplate.queryForList(accountSelectSql, Long.class);
        List<Long> assgrpIds = fiJdbcTemplate.queryForList(assgrpSelectSql, Long.class);
        AssGrpBuilder assGrpBuilder = new AssGrpBuilder(periodVOList, assgrpIds);
        List<Long> mainCFItemIds = containsCashFlow ? fiJdbcTemplate.queryForList(mainCFItemSelectSql, Long.class) : Collections.emptyList();
        List<Long> suppCFItemIds = containsCashFlow ? fiJdbcTemplate.queryForList(suppCFItemSelectSql, Long.class) : Collections.emptyList();
        List<SubIrrigateTaskExecutable> result = new ArrayList<>(orgIds.size() * periodVOList.size());
        for (BookService.BookVO bookVO: bookVOList) {
            for (PeriodVOBuilder.PeriodVO periodVO : periodVOList) {
                // 120230010 -> 2301
                int periodNumber = (int) ((periodVO.getId() / 1_0000) % 100 * 100 + ((periodVO.getId() / 10) % 100));
                SubIrrigateTaskExecutable subIrrigateTaskExecutable = SubIrrigateTaskExecutable.builder()
                        .entryCurrencyId(entryCurrencyId)
                        .entryRatio(entryRatio)
                        .repetition(repetition)
                        .containsVoucher(containsVoucher)
                        .containsBalance(containsBalance)
                        .containsSumBalance(containsSumBalance)
                        .containsVoucherCount(containsVoucherCount)
                        .containsCashFlow(containsCashFlow)
                        .bookVO(bookVO)
                        .periodVO(periodVO)
                        .accountIds(accountIds)
                        .assgrpIds(assGrpBuilder.getAssGrpIds(periodVO.getId()))
                        .mainCFItemIds(mainCFItemIds)
                        .suppCFItemIds(suppCFItemIds)
                        .voucherShardingIndex(VoucherShardingService.getShardingIndex(bookVO.getOrgId(), periodVO.getId()))
                        .localRate(bookVO.getLocalCurrencyId() == entryCurrencyId ? 1 : 10)
                        .xorSuffix(bookVO.getOrgId() ^ periodVO.getId() ^ entryCurrencyId)
                        // fid : distinctSeq(1) + bookIndex(5) + periodNumber(intercept last 4) + voucherHeadCount(5)
                        .beginVoucherId(distinctSign * 100_0000_0000_0000L + bookVO.getIndex() * 10_0000_0000L + periodNumber * 10_0000)
                        .rowsBuilder(new RowsBuilder(bookVO, periodVO, entryCurrencyId))
                        .bookedDateRangeList(DateUtils.generateDateRange(periodVO.getBeginDate(), periodVO.getEndDate()))
                        .build();
                checkSubTask(subIrrigateTaskExecutable);
                result.add(subIrrigateTaskExecutable);
            }
        }
        return result;
    }

    // check
    private void checkSubTask(SubIrrigateTaskExecutable subIrrigateTaskExecutable) {
        int CCIDSize = subIrrigateTaskExecutable.getAccountIds().size() * subIrrigateTaskExecutable.getAssgrpIds().size();
        // CCID必须为偶数
        Assert.isTrue((CCIDSize & 1) == 0, "CCID count is required even number.");
        // 头行比必须为偶数
        Assert.isTrue((entryRatio & 1) == 0, "entryRatio must be even number");
        // 勾选了现金余额，必须要有现金流量项目
        Assert.isTrue(!subIrrigateTaskExecutable.containsCashFlow ||
                (!subIrrigateTaskExecutable.getMainCFItemIds().isEmpty() && !subIrrigateTaskExecutable.getSuppCFItemIds().isEmpty()),
                "mainCFItemIds or suppCFItemIds can not be empty when containsCash!");
    }

}
