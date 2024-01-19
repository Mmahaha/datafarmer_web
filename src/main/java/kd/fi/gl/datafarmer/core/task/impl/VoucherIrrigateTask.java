package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.bean.AcctSumAmtInfo;
import kd.fi.gl.datafarmer.core.bean.AmtInfo;
import kd.fi.gl.datafarmer.core.bean.CashInfo;
import kd.fi.gl.datafarmer.core.bean.CashSumInfo;
import kd.fi.gl.datafarmer.core.util.BookService;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.DateUtils;
import kd.fi.gl.datafarmer.core.util.FastStringUtils;
import kd.fi.gl.datafarmer.core.util.PeriodVOBuilder;
import kd.fi.gl.datafarmer.core.util.RowsBuilder;
import kd.fi.gl.datafarmer.core.util.VoucherCountAccumulator;
import kd.fi.gl.datafarmer.core.util.helper.CopyHelper;
import kd.fi.gl.datafarmer.core.util.sharding.BalanceShardingService;
import kd.fi.gl.datafarmer.core.util.sharding.VoucherShardingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static kd.fi.gl.datafarmer.core.task.impl.VoucherIrrigateTask.IrrigateResult;

/**
 * 凭证灌输任务，按组织+期间进行构造
 */
@Slf4j
public class VoucherIrrigateTask implements Callable<IrrigateResult> {

    private final int repetition;   // 重复度
    private final int entryRatio;   // 头行比
    private final List<Long> accountIds;    // 科目id集合
    private final List<Long> assgrpIds;     // 纬度id集合
    private final int localRate;            // 分录行汇率
    private final long xorSuffix;           // 异或前缀，用于后面和科目纬度后续异或以生成金额
    private final long beginVoucherId;      // 凭证起始id，形如100012023011:去重标识(1)+组织序号(4)+期间编码(6)+科目类型(1)+凭证序号(4)
    private final RowsBuilder rowsBuilder;  // 行构造器
    protected CopyHelper copyHelper;    // copyIn帮助类，声明为protected以便单元测试
    private final String taskIdentity;      // 任务标识，代表此任务的纬度
    private List<String> bookedDateRangeList;   // 记账日期范围
    private int bookedDateIndex = 0;        // 记账日期遍历指针，不断移动重复
    private int mainCfItemIndex = 0;        // 主表项目遍历指针，不断移动并重复
    private int suppCfItemIndex = 0;        // 附表项目遍历指针，不断移动并重复
    private VoucherCountAccumulator voucherCountAccumulator;    // 凭证计数器
    private final List<Long> mainCfItemIds;
    private final List<Long> suppCfItemIds;
    private final boolean isCash;
    private static final CashInfo zeroCashInfo = new CashInfo(0,0,0);
    private final int voucherShardingIndex;
    private final int balanceShardingIndex;

    /**
     * 凭证/余额灌输任务
     *
     * @param bookVO          账簿对象
     * @param periodVO        期间对象
     * @param entryCurrencyId 分录币别id
     * @param entryRatio      凭证总分录数
     * @param accountIds      科目id集合
     * @param assgrpIds       核算纬度id集合，与科目形成笛卡尔积
     * @param repetition      重复度：每个科目+纬度 重复使用的次数
     */
    public VoucherIrrigateTask(BookService.BookVO bookVO, PeriodVOBuilder.PeriodVO periodVO, long entryCurrencyId, int entryRatio,
                               List<Long> accountIds, List<Long> assgrpIds, int repetition, boolean isCash,
                               List<Long> mainCfItemIds, List<Long> suppCfItemIds, int distinctSign, String accountType) {
        // 头行比必须为偶数
        Assert.isTrue((entryRatio & 1) == 0, "entryRatio must be even!");
        // 科目 * 纬度 的数量需要为偶数
        Assert.isTrue(((accountIds.size() * assgrpIds.size()) & 1) == 0, "accountIds multiply assgrpIds must be even!");
        this.taskIdentity = String.format("orgId:%d,periodId:%d,currencyId:%d,accountIdSize:%s,assgrpIdSize:%s,repetition:%d,entryRatio:%d,accountType:%s",
                bookVO.getId(), periodVO.getId(), entryCurrencyId, accountIds.size(), assgrpIds.size(), repetition, entryRatio, accountType);
        // 是现金类型时，主附表不能为空
//        Preconditions.checkArgument(!isCash || (!mainCfItemIds.isEmpty() && !suppCfItemIds.isEmpty()), "mainCfItemIds can not be empty when isCash!");
        voucherShardingIndex = VoucherShardingService.getShardingIndex(bookVO.getOrgId(), periodVO.getId());
        balanceShardingIndex = BalanceShardingService.getShardingIndex(bookVO.getOrgId());
        long periodId = periodVO.getId();
        this.repetition = repetition;
        this.entryRatio = entryRatio;
        this.accountIds = accountIds;
        this.assgrpIds = assgrpIds;
        this.localRate = bookVO.getLocalCurrencyId() == entryCurrencyId ? 1 : 10;
        this.xorSuffix = bookVO.getOrgId() ^ periodId ^ entryCurrencyId;
        int periodNumber = (int) ((periodId / 1_0000) % 1_0000 * 100 + ((periodId / 10) % 100)); // 202201
        this.beginVoucherId = distinctSign * 1_000_0000_0000_0000L + bookVO.getIndex() * 1_000_0000_0000L
                + periodNumber * 10_0000L + Integer.parseInt(accountType) * 1_0000L;
        this.rowsBuilder = new RowsBuilder(bookVO, periodVO, entryCurrencyId);
        this.bookedDateRangeList = DateUtils.generateDateRange(periodVO.getBeginDate(), periodVO.getEndDate());
        this.voucherCountAccumulator = new VoucherCountAccumulator();
        this.isCash = isCash;
        this.mainCfItemIds = mainCfItemIds;
        this.suppCfItemIds = suppCfItemIds;
    }

    @Override
    public IrrigateResult call() throws Exception {
        try {
            this.copyHelper = DB.getCopyHelper();
//            log.info("开始执行任务，任务标识:{}", taskIdentity);
            return callInternal();
        } catch (Exception e) {
            log.error("执行任务出现异常,taskIdentity=" + taskIdentity, e);
            throw e;
        } finally {
            if (copyHelper != null) {
                copyHelper.close();
            }
        }
    }

    private IrrigateResult callInternal() {
//        long startTick = System.currentTimeMillis();
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
        CashInfo cashInfo = zeroCashInfo;
        Map<Long, AcctSumAmtInfo> acctSumAmtInfoMap = new HashMap<>(accountIds.size());
        Map<Long, CashSumInfo> cashSumInfoMap = new HashMap<>(100);
        while (--loopTimes >= 0) {
            for (Long assgrpId : assgrpIds) {
                for (Long accountId : accountIds) {
                    boolean isEquity = false;   //是否权益类
                    // 奇数行切换一次金额信息，偶数行切换一次现金流量信息
                    if ((entrySeq & 1) == 0) {
                        amtInfo = nextAmtInfo(accountId, assgrpId);
                        if (isCash) {
                            cashInfo = zeroCashInfo; //下一行为现金，重置
                        }
                    } else if (isCash) {
                        cashInfo = nextCashInfo(amtInfo.getLocAmt());
                        isEquity = true;  // 下面要处理的行为权益类，无纬度
                    }
                    // 构造分录，先借后贷
                    entryStrRows.add(rowsBuilder.buildVoucherEntry(entryId = nextEntryId(voucherId, ++entrySeq),
                            voucherId, entrySeq, accountId, isEquity ? 0L : assgrpId, (entrySeq & 1) == 1, amtInfo, cashInfo));
                    // 只在最后一次重复度循环里处理余额构造
                    if (loopTimes == 0) {
                        Object[] params = new Object[] {amtInfo, entrySeq, entryId, isEquity};  // for lambda
                        acctSumAmtInfoMap.compute(accountId, (accId, acctSumAmtInfo) -> {
                            if (acctSumAmtInfo == null) {acctSumAmtInfo = new AcctSumAmtInfo(accId, (Long) params[2], (boolean) params[3]);}
                            acctSumAmtInfo.add((AmtInfo) params[0], ((int)params[1] & 1) == 1);
                            return acctSumAmtInfo;
                        });
                        if (!isEquity) { // 权益类先不写余额，后续任务再从汇总余额拷贝
                            balance$PkRows.add(rowsBuilder.buildBalance$Pk(entryId, 9));
                            balanceStrRows.add(rowsBuilder.buildBalance(entryId, accountId, assgrpId, (entrySeq & 1) == 1, amtInfo, repetition));
                        }
                        if (isCash && cashInfo != zeroCashInfo) {
                            // 主表
                            cashSumInfoMap.compute(cashInfo.getMainCfItemId(), (cfItemId,cashSumInfo) -> {
                                if (cashSumInfo == null) {cashSumInfo = new CashSumInfo((Long) params[2], cfItemId);}
                                cashSumInfo.add(((AmtInfo)params[0]).getLocAmt());
                                return cashSumInfo;});
                            // 附表
                            cashSumInfoMap.compute(cashInfo.getSuppCfItemId(), (cfItemId,cashSumInfo) -> {
                                if (cashSumInfo == null) {cashSumInfo = new CashSumInfo((Long) params[2] + 1, cfItemId);}
                                cashSumInfo.add(((AmtInfo)params[0]).getLocAmt());
                                return cashSumInfo;});
                        }
                    }
                    if (entrySeq == entryRatio) {
                        // 构造头
                        headStrRows.add(rowsBuilder.buildVoucherHead(voucherIndex, voucherId, billno, curDate = nextDate(), isCash));
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
//            log.info("为最后一批不到头行比的分录补充凭证头,taskIdentity={}", taskIdentity);
            headStrRows.add(rowsBuilder.buildVoucherHead(voucherIndex, voucherId, billno, curDate = nextDate(), isCash));
            voucherCountAccumulator.accumulate(curDate, 1, entrySeq, voucherId);
        }
        // 保存最后一批数据
        saveAndClearRows(result, headStrRows, entryStrRows, balanceStrRows, cashBalanceStrRows, $pkStrRows, balance$PkRows);
        // 单独计算汇总余额
        for (AcctSumAmtInfo acctSumAmtInfo : acctSumAmtInfoMap.values()) {
            sumBalanceStrRows.add(rowsBuilder.buildSumBalance(acctSumAmtInfo.getFid(), acctSumAmtInfo, repetition, assgrpIds.size()));
        }
        // 单独计算现金流量
        for (CashSumInfo cashSumInfo : cashSumInfoMap.values()) {
            cashBalanceStrRows.add(rowsBuilder.buildCashFlow(cashSumInfo.getFid(), cashSumInfo.getCfItemId(), cashSumInfo.getAmount(), repetition));
        }
        copyHelper.copyCashFlow(cashBalanceStrRows);
        result.sumBalanceCount += copyHelper.copySumBalance(sumBalanceStrRows);
        // 单独保存凭证计数
        voucherCountAccumulator.getVoucherCountMap().forEach((bookedDate, voucherCount) -> {
            voucherCountStrRows.add(rowsBuilder.buildVoucherCount(bookedDate, voucherCount));
        });
        copyHelper.copyVoucherCount(voucherCountStrRows);
//        log.info("任务执行完成，result = {}, taskIdentity = {}", result, taskIdentity);
//        CoreLogger.LOG.info("import data successfully, taskIdentity: {}, voucher count: {}, entry count: {}, balance count: {}, sumBalance count:{}, cost {} seconds.",
//                this.taskIdentity, result.getVoucherCount(), result.getEntryCount(),
//                result.getBalanceCount(), result.getSumBalanceCount(), (System.currentTimeMillis() - startTick) / 1000);
        return result;
    }

    private CashInfo nextCashInfo(int locAmt) {
        CashInfo result = new CashInfo(mainCfItemIds.get(mainCfItemIndex), suppCfItemIds.get(suppCfItemIndex), locAmt);
        mainCfItemIndex = (mainCfItemIndex + 1) % mainCfItemIds.size();
        suppCfItemIndex = (suppCfItemIndex + 1) % suppCfItemIds.size();
        return result;
    }

    private void saveAndClearRows(IrrigateResult result, List<String> headStrRows, List<String> entryStrRows,
                                  List<String> balanceStrRows, List<String> cashBalanceStrRows, List<String> $pkStrRows,
                                  List<String> balance$PkStrRows) {
        long start = System.currentTimeMillis();
        result.voucherCount += copyHelper.copyVoucherHead(voucherShardingIndex, headStrRows);
        copyHelper.copyVoucher$PK($pkStrRows);
        result.entryCount += copyHelper.copyVoucherEntry(voucherShardingIndex, entryStrRows);
        result.balanceCount += copyHelper.copyBalance(balanceShardingIndex, balanceStrRows);
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
        return beginVoucherId + voucherIndex;
    }

    private long nextEntryId(long voucherId, int entrySeq) {
        return voucherId * 100 + entrySeq;
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
