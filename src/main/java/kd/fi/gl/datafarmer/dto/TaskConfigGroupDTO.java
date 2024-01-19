package kd.fi.gl.datafarmer.dto;

import kd.fi.gl.datafarmer.core.util.BookService;
import kd.fi.gl.datafarmer.core.util.PeriodVOBuilder;
import kd.fi.gl.datafarmer.model.TaskConfig;

import java.util.List;

/**
 * Description: task config group中间层对象，未拆分组织期间
 *
 * @author ysj
 * @date 2024/1/17
 */
public class TaskConfigGroupDTO {

    private List<BookService.BookVO> bookVOS;
    private List<PeriodVOBuilder.PeriodVO> periodVOS;
    private long entryCurrencyId;
    private int entryRatio;
    private int repetition;
    private List<Long> accountIds;
    private List<Long> assgrpIds;
    private List<Long> mainCFItemIds;
    private List<Long> suppCFItemIds;
    private boolean containsVoucher;
    private boolean containsBalance;
    private boolean containsSumBalance;
    private boolean containsVoucherCount;
    private boolean containsCashFlow;


    public TaskConfigGroupDTO() {
    }

    public TaskConfigGroupDTO(TaskConfig taskConfig) {
        this.entryCurrencyId = taskConfig.getEntryCurrencyId();
        this.entryRatio = taskConfig.getEntryRatio();
        this.repetition = taskConfig.getRepetition();
        this.containsVoucher = taskConfig.isContainsVoucher();
        this.containsBalance = taskConfig.isContainsBalance();
        this.containsBalance = taskConfig.isContainsBalance();
        this.containsSumBalance = taskConfig.isContainsSumBalance();
        this.containsVoucherCount = taskConfig.isContainsVoucherCount();
        this.containsCashFlow = taskConfig.isContainsCashFlow();
    }

    public long getEntryCurrencyId() {
        return entryCurrencyId;
    }

    public void setEntryCurrencyId(long entryCurrencyId) {
        this.entryCurrencyId = entryCurrencyId;
    }

    public int getEntryRatio() {
        return entryRatio;
    }

    public void setEntryRatio(int entryRatio) {
        this.entryRatio = entryRatio;
    }

    public int getRepetition() {
        return repetition;
    }

    public void setRepetition(int repetition) {
        this.repetition = repetition;
    }

    public List<BookService.BookVO> getBookVOS() {
        return bookVOS;
    }

    public void setBookVOS(List<BookService.BookVO> bookVOS) {
        this.bookVOS = bookVOS;
    }

    public List<PeriodVOBuilder.PeriodVO> getPeriodVOS() {
        return periodVOS;
    }

    public void setPeriodVOS(List<PeriodVOBuilder.PeriodVO> periodVOS) {
        this.periodVOS = periodVOS;
    }

    public List<Long> getAccountIds() {
        return accountIds;
    }

    public void setAccountIds(List<Long> accountIds) {
        this.accountIds = accountIds;
    }

    public List<Long> getAssgrpIds() {
        return assgrpIds;
    }

    public void setAssgrpIds(List<Long> assgrpIds) {
        this.assgrpIds = assgrpIds;
    }

    public List<Long> getMainCFItemIds() {
        return mainCFItemIds;
    }

    public void setMainCFItemIds(List<Long> mainCFItemIds) {
        this.mainCFItemIds = mainCFItemIds;
    }

    public List<Long> getSuppCFItemIds() {
        return suppCFItemIds;
    }

    public void setSuppCFItemIds(List<Long> suppCFItemIds) {
        this.suppCFItemIds = suppCFItemIds;
    }

    public boolean isContainsVoucher() {
        return containsVoucher;
    }

    public void setContainsVoucher(boolean containsVoucher) {
        this.containsVoucher = containsVoucher;
    }

    public boolean isContainsBalance() {
        return containsBalance;
    }

    public void setContainsBalance(boolean containsBalance) {
        this.containsBalance = containsBalance;
    }

    public boolean isContainsSumBalance() {
        return containsSumBalance;
    }

    public void setContainsSumBalance(boolean containsSumBalance) {
        this.containsSumBalance = containsSumBalance;
    }

    public boolean isContainsVoucherCount() {
        return containsVoucherCount;
    }

    public void setContainsVoucherCount(boolean containsVoucherCount) {
        this.containsVoucherCount = containsVoucherCount;
    }

    public boolean isContainsCashFlow() {
        return containsCashFlow;
    }

    public void setContainsCashFlow(boolean containsCashFlow) {
        this.containsCashFlow = containsCashFlow;
    }


}
