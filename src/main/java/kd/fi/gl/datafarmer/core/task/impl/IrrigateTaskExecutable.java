package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.common.exception.impl.ParseConfigException;
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
import java.util.stream.Collectors;
import java.util.stream.LongStream;

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
    protected boolean containsVoucher = true;
    protected boolean containsBalance = true;
    protected boolean containsSumBalance = true;
    protected boolean containsVoucherCount = true;
    protected boolean containsCashFlow = false;
    private String orgSelectSql;
    private String startPeriodNumber;
    private String endPeriodNumber;
    private String accountSelectSql;
    private String cashAccountSelectSql;
    private String assgrpSelectSql;
    private String mainCFItemSelectSql;
    private String suppCFItemSelectSql;
    private String mainCFAssgrpSelectSql;
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
        List<Long> orgIds = fetchBaseData(orgSelectSql, "组织");
        List<BookService.BookVO> bookVOList = BookService.getBookVOsByOrg(orgIds);
        List<PeriodVOBuilder.PeriodVO> periodVOList = new PeriodVOBuilder(1, startPeriodNumber, endPeriodNumber).getPeriodVOList();
        List<Long> accountIds = fetchBaseData(accountSelectSql, "会计科目");
        List<Long> cashAccountIds = fetchBaseData(cashAccountSelectSql, "现金科目");
        if (containsCashFlow && accountIds.size() != cashAccountIds.size()) {
            throw new IllegalArgumentException("现金科目数量与其他科目数量不一致");
        }
        if (containsCashFlow) {
            // 现金科目交错插入到原科目上
            List<Long> finalAccountIds = accountIds;
            accountIds = LongStream.range(0, accountIds.size())
                    .flatMap(i -> LongStream.of(cashAccountIds.get((int) i), finalAccountIds.get((int) i)))
                    .boxed().collect(Collectors.toList());
        }
        List<Long> assgrpIds = fetchBaseData(assgrpSelectSql, "核算维度");
        AssGrpBuilder assGrpBuilder = new AssGrpBuilder(periodVOList, assgrpIds);
        List<Long> mainCFItemIds = fetchBaseData(mainCFItemSelectSql, "主表项目");
        List<Long> suppCFItemIds = fetchBaseData(suppCFItemSelectSql, "附表项目");
        List<Long> mainCFAssgrpIds = fetchBaseData(mainCFAssgrpSelectSql, "主表核算维度");
        AssGrpBuilder mainCFAssGrpBuilder = new AssGrpBuilder(periodVOList, mainCFAssgrpIds);
        List<SubIrrigateTaskExecutable> result = new ArrayList<>(orgIds.size() * periodVOList.size());
        for (BookService.BookVO bookVO: bookVOList) {
            for (PeriodVOBuilder.PeriodVO periodVO : periodVOList) {
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
                        .mainCFAssgrpIds(mainCFAssGrpBuilder.getAssGrpIds(periodVO.getId()))
                        .voucherShardingIndex(VoucherShardingService.getShardingIndex(bookVO.getOrgId(), periodVO.getId()))
                        .localRate(bookVO.getLocalCurrencyId() == entryCurrencyId ? 1 : 10)
                        .xorSuffix(bookVO.getOrgId() ^ periodVO.getId() ^ entryCurrencyId)
                        // fid : distinctSeq(2) + bookIndex(5) + periodNumber(intercept last 4) + voucherHeadCount(5) (+entrySeq(3))
//                        .beginVoucherId(distinctSign * 100_0000_0000_0000L + bookVO.getIndex() * 10_0000_0000L + periodNumber * 10_0000)
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
                (!subIrrigateTaskExecutable.getMainCFItemIds().isEmpty() && !subIrrigateTaskExecutable.getMainCFAssgrpIds().isEmpty()),
                "mainCFItemIds or suppCFItemIds can not be empty when containsCash!");
    }

    private List<Long> fetchBaseData(String selectSql, String field) {
        try {
            if (selectSql == null || selectSql.trim().length() == 0) {
                return Collections.emptyList();
            }
            String[] split = selectSql.split(";");
            JdbcTemplate jdbcTemplate = DB.getJdbcTemplate(split[0]);
            List<Long> result = jdbcTemplate.queryForList(split[1], Long.class);
            Assert.isTrue(Integer.parseInt(split[2]) == result.size(),
                    String.format("查询结果与预期数量不符，预期数量%s，实际数量%s", split[2], result.size()));
            return result;
        } catch (Exception e) {
            throw new ParseConfigException(field + "字段解析异常：" + e.getClass().getName() + ":" + e.getMessage(), e);
        }
    }

}
