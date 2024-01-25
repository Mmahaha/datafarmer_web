package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.bean.AcctSumAmtInfo;
import kd.fi.gl.datafarmer.core.bean.AmtInfo;
import kd.fi.gl.datafarmer.core.bean.CashInfo;
import kd.fi.gl.datafarmer.core.bean.CashSumInfo;
import kd.fi.gl.datafarmer.core.util.BookService;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.FastStringUtils;
import kd.fi.gl.datafarmer.core.util.ID;
import kd.fi.gl.datafarmer.core.util.PeriodVOBuilder;
import kd.fi.gl.datafarmer.core.util.RowsBuilder;
import kd.fi.gl.datafarmer.core.util.VoucherCountAccumulator;
import kd.fi.gl.datafarmer.core.util.helper.CopyHelper;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Description: 灌数任务的子任务，按组织+期间拆分
 *
 * @author ysj
 * @date 2024/1/21
 */

@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@Data
@Slf4j
public class SubIrrigateTaskExecutable extends IrrigateTaskExecutable {

    private BookService.BookVO bookVO;
    private PeriodVOBuilder.PeriodVO periodVO;
    private List<Long> accountIds;
    private List<Long> assgrpIds;
    private List<Long> mainCFItemIds;
    private List<Long> suppCFItemIds;
    private List<Long> mainCFAssgrpIds;
    private int voucherShardingIndex;
    private int localRate;
    private long xorSuffix;
//    private long beginVoucherId;
    private RowsBuilder rowsBuilder;
    private String taskIdentity;
    private List<String> bookedDateRangeList;
    private int bookedDateIndex = 0;
    private Iterator<Long> mainCFItemIter;
    private Iterator<Long> mainCFAssgrpIter;
    private final VoucherCountAccumulator voucherCountAccumulator = new VoucherCountAccumulator();

    // LazyInit
    @Setter(AccessLevel.NONE)
    private CopyHelper copyHelper;

    @Override
    public boolean supportSplit() {
        return false;
    }

    @Override
    public void execute() {
        try {
            log.info("开始执行任务,orgId={},periodId={}", bookVO.getOrgId(), periodVO.getId());
            this.copyHelper = DB.getCopyHelper();
            executeInternal();
            log.info("任务执行完成,orgId={},periodId={}", bookVO.getOrgId(), periodVO.getId());
        } catch (Exception e) {
            log.error("执行任务出现异常,taskIdentity=" + taskIdentity, e);
            throw new RuntimeException("执行任务出现异常", e);
        } finally {
            if (copyHelper != null) {
                copyHelper.close();
            }
        }
    }

    private void executeInternal() {
        List<String> headStrRows = new ArrayList<>(10000 / entryRatio + 1);
        List<String> $pkStrRows = new ArrayList<>(10000 / entryRatio + 1);
        List<String> entryStrRows = new ArrayList<>(10000);
        List<String> balanceStrRows = new ArrayList<>(10000);
        List<String> cashBalanceStrRows = new ArrayList<>(10000);
        List<String> balance$PkRows = new ArrayList<>(10000);
        List<String> sumBalanceStrRows = new ArrayList<>(accountIds.size());    // 科目汇总余额
        List<String> voucherCountStrRows = new ArrayList<>(10000);
        IrrigateResult result = new IrrigateResult();
        int voucherIndex = 0;  // 凭证头序号，不断自增，用于填充凭证号和id
        int entrySeq = 0;   // 分录序号，使用过程中不断重置
        long voucherId = nextVoucherId(++voucherIndex); // 凭证id，使用过程不断重置
        String billno = nextBillno(voucherIndex);   // 凭证号，使用过程不断重置
        long entryId = 0;   // 分录id
        // 总分录数，用来取余运算
        AmtInfo amtInfo = null;
        int loopTimes = repetition;
        String curDate;
        CashInfo cashInfo = CashInfo.ZERO;
        Map<Long, AcctSumAmtInfo> acctSumAmtInfoMap = new HashMap<>(accountIds.size());
        while (--loopTimes >= 0) {
            for (Long assgrpId : assgrpIds) {
                for (Long accountId : accountIds) {
                    // 下一行为奇数行切换一次金额信息，偶数行切换一次现金流量信息
                    if ((entrySeq & 1) == 0) {
                        amtInfo = nextAmtInfo(accountId, assgrpId);
                        if (containsCashFlow) {
                            cashInfo = CashInfo.ZERO; //下一行为现金，重置
                        }
                    } else if (containsCashFlow) {
                        cashInfo = nextCashInfo(amtInfo.getLocAmt());
                    }
                    // 构造分录，先借后贷
                    entryStrRows.add(rowsBuilder.buildVoucherEntry(entryId = nextEntryId(voucherId, ++entrySeq),
                            voucherId, entrySeq, accountId, assgrpId, (entrySeq & 1) == 1, amtInfo, cashInfo));
                    if (containsCashFlow && cashInfo != CashInfo.ZERO) {
                        cashBalanceStrRows.add(rowsBuilder.buildCashFlow(entryId, cashInfo.getMainCfItemId(), cashInfo.getMainCFAssgrpId(), cashInfo.getCfAmount(), 1));
                    }
                    // 只在最后一次重复度循环里处理余额构造
                    if (loopTimes == 0) {
//                        Object[] params = new Object[] {amtInfo, entrySeq, entryId};  // for lambda
//                        acctSumAmtInfoMap.compute(accountId, (accId, acctSumAmtInfo) -> {
//                            if (acctSumAmtInfo == null) {acctSumAmtInfo = new AcctSumAmtInfo(accId, (Long) params[2], (boolean) params[3]);}
//                            acctSumAmtInfo.add((AmtInfo) params[0], ((int)params[1] & 1) == 1);
//                            return acctSumAmtInfo;
//                        });
                        balanceStrRows.add(rowsBuilder.buildBalance(entryId, accountId, assgrpId, (entrySeq & 1) == 1, amtInfo, repetition));
                    }
                    if (entrySeq == entryRatio) {
                        // 构造头
                        headStrRows.add(rowsBuilder.buildVoucherHead(voucherIndex, voucherId, billno, curDate = nextDate(), containsCashFlow));
                        // 累计凭证计数
                        voucherCountAccumulator.accumulate(curDate, 1, entrySeq, voucherId);
                        // 构造快速索引
                        $pkStrRows.add(rowsBuilder.buildVoucher$PK(voucherId, voucherShardingIndex, billno));
                        // 切换至下一张凭证
                        voucherId = nextVoucherId(++voucherIndex);
                        billno = FastStringUtils.addLeadingZeros(voucherIndex, 8);
                        entrySeq = 0;
                        // 分录达到1w，保存所有数据
                        if (entryStrRows.size() >= 10000) {
                            saveAndClearRows(result, headStrRows, entryStrRows, balanceStrRows, cashBalanceStrRows, $pkStrRows, balance$PkRows);
                        }
                    }
                }
            }
        }
        // 最后一批分录的凭证头如果还没生成，则进行补充
        if (entrySeq != 0) {
            headStrRows.add(rowsBuilder.buildVoucherHead(voucherIndex, voucherId, billno, curDate = nextDate(), containsCashFlow));
            voucherCountAccumulator.accumulate(curDate, 1, entrySeq, voucherId);
        }
        // 保存最后一批数据
        saveAndClearRows(result, headStrRows, entryStrRows, balanceStrRows, cashBalanceStrRows, $pkStrRows, balance$PkRows);
        // 单独计算汇总余额
//        for (AcctSumAmtInfo acctSumAmtInfo : acctSumAmtInfoMap.values()) {
//            sumBalanceStrRows.add(rowsBuilder.buildSumBalance(acctSumAmtInfo.getFid(), acctSumAmtInfo, repetition, assgrpIds.size()));
//        }
        copyHelper.copyCashFlow(cashBalanceStrRows);
        result.sumBalanceCount += copyHelper.copySumBalance(sumBalanceStrRows);
        // 单独保存凭证计数
        voucherCountAccumulator.getVoucherCountMap().forEach((bookedDate, voucherCount) -> {
            voucherCountStrRows.add(rowsBuilder.buildVoucherCount(bookedDate, voucherCount));
        });
        copyHelper.copyVoucherCount(voucherCountStrRows);
    }

    private long curMainCFItemId = 0L;
    private CashInfo nextCashInfo(int locAmt) {
        // 获取下一行现金行信息，主表项目只遍历依次，主表纬度循环遍历直到主表项目遍历完毕
        if (mainCFItemIter == null) {
            // init
            mainCFItemIter = mainCFItemIds.iterator();
            curMainCFItemId = mainCFItemIter.next();
            mainCFAssgrpIter = mainCFAssgrpIds.iterator();
        }
        if (!mainCFAssgrpIter.hasNext() && !mainCFItemIter.hasNext()) {
            // 两个都遍历完了，后续的分录行就不再录入现金流量
            return CashInfo.ZERO;
        }
        if (!mainCFAssgrpIter.hasNext()) {
            // 切换到下一个主表项目，同时重新遍历主表纬度
            curMainCFItemId = mainCFItemIter.next();
            mainCFAssgrpIter = mainCFAssgrpIds.iterator();
        }
        return new CashInfo(curMainCFItemId, 0L, mainCFAssgrpIter.hasNext() ? mainCFAssgrpIter.next() : 0L, locAmt);
    }

    private void saveAndClearRows(IrrigateResult result, List<String> headStrRows, List<String> entryStrRows,
                                  List<String> balanceStrRows, List<String> cashBalanceStrRows, List<String> $pkStrRows,
                                  List<String> balance$PkStrRows) {
        long start = System.currentTimeMillis();
        result.voucherCount += copyHelper.copyVoucherHead(voucherShardingIndex, headStrRows);
        copyHelper.copyVoucher$PK($pkStrRows);
        result.entryCount += copyHelper.copyVoucherEntry(voucherShardingIndex, entryStrRows);
        result.balanceCount += copyHelper.copyBalance(balanceStrRows);
        copyHelper.copyBalance$PK(balance$PkStrRows);
        result.cashFlowCount += copyHelper.copyCashFlow(cashBalanceStrRows);
//        log.info("{}行凭证及关联数据插入耗时{}ms", entryStrRows.size(), System.currentTimeMillis() - start);
        headStrRows.clear();
        entryStrRows.clear();
        balanceStrRows.clear();
        cashBalanceStrRows.clear();
        $pkStrRows.clear();
        balance$PkStrRows.clear();
    }


    private long nextVoucherId(int voucherIndex) {
        return ID.genLongId();
    }

    private long nextEntryId(long voucherId, int entrySeq) {
        return ID.genLongId();
    }

    private String nextBillno(int voucherIndex) {
        return FastStringUtils.addLeadingZeros(voucherIndex, 8);
    }

    // 按照唯一纬度生成指定的分录金额，确保每次重复度下的金额一致
    private AmtInfo nextAmtInfo(long accountId, long assgrpId) {
        int oriAmt = (int) ((xorSuffix ^ accountId ^ assgrpId) % 9901 + 100);    //[100,10000]
        return new AmtInfo(oriAmt, localRate,oriAmt * localRate);
    }

    private String nextDate() {
        String result = bookedDateRangeList.get(bookedDateIndex);
        bookedDateIndex = (bookedDateIndex + 1) % bookedDateRangeList.size();
        return result;
    }


    public static class IrrigateResult {
        private long voucherCount;
        private long entryCount;
        private long balanceCount;
        private long cashFlowCount;
        private long sumBalanceCount;

        public long getVoucherCount() {
            return voucherCount;
        }

        public long getEntryCount() {
            return entryCount;
        }

        public long getBalanceCount() {
            return balanceCount;
        }

        public long getCashFlowCount() {
            return cashFlowCount;
        }

        public long getSumBalanceCount() {
            return sumBalanceCount;
        }

        @Override
        public String toString() {
            return "IrrigateResult{" +
                    "voucherCount=" + voucherCount +
                    ", entryCount=" + entryCount +
                    ", balanceCount=" + balanceCount +
                    ", cashFlowCount=" + cashFlowCount +
                    ", sumBalanceCount=" + sumBalanceCount +
                    '}';
        }
    }


}
