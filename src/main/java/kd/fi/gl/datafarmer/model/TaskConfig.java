package kd.fi.gl.datafarmer.model;


/**
 * Description:任务的数据库实体类
 *
 * @author ysj
 * @date 2024/1/14
 */

public class TaskConfig {


    private long id;
    private String orgSelectSql;
    private long entryCurrencyId;
    private int entryRatio;
    private int repetition;
    private String startPeriodNumber;
    private String endPeriodNumber;
    private String accountSelectSql;
    private String assgrpSelectSql;
    private boolean containsVoucher;
    private boolean containsBalance;
    private boolean containsSumBalance;
    private boolean containsVoucherCount;
    private boolean containsCashFlow;


    public void setId(long id) {
        this.id = id;
    }

    public long getId() {
        return id;
    }

    public String getOrgSelectSql() {
        return orgSelectSql;
    }

    public void setOrgSelectSql(String orgSelectSql) {
        this.orgSelectSql = orgSelectSql;
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

    public String getStartPeriodNumber() {
        return startPeriodNumber;
    }

    public void setStartPeriodNumber(String startPeriodNumber) {
        this.startPeriodNumber = startPeriodNumber;
    }

    public String getEndPeriodNumber() {
        return endPeriodNumber;
    }

    public void setEndPeriodNumber(String endPeriodNumber) {
        this.endPeriodNumber = endPeriodNumber;
    }

    public String getAccountSelectSql() {
        return accountSelectSql;
    }

    public void setAccountSelectSql(String accountSelectSql) {
        this.accountSelectSql = accountSelectSql;
    }

    public String getAssgrpSelectSql() {
        return assgrpSelectSql;
    }

    public void setAssgrpSelectSql(String assgrpSelectSql) {
        this.assgrpSelectSql = assgrpSelectSql;
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
