package kd.fi.gl.datafarmer.core.bean;

/**
 * 科目汇总余额
 */
public class AcctSumAmtInfo {

    private long fid;

    private long accountId;

    private long debitFor;

    private long debitLoc;

    private long creditFor;

    private long creditLoc;

    private boolean nonAssgrp;

    public AcctSumAmtInfo(long accountId, long fid, boolean nonAssgrp) {
        this.accountId = accountId;
        this.fid = fid;
        this.nonAssgrp = nonAssgrp;
    }

    public void add(AmtInfo amtInfo, boolean debitDC) {
        if (debitDC) {
            debitFor += amtInfo.getOriAmt();
            debitLoc += amtInfo.getLocAmt();
        } else {
            creditFor += amtInfo.getOriAmt();
            creditLoc += amtInfo.getLocAmt();
        }
    }

    public long getAccountId() {
        return accountId;
    }

    public long getDebitFor() {
        return debitFor;
    }

    public long getDebitLoc() {
        return debitLoc;
    }

    public long getCreditFor() {
        return creditFor;
    }

    public long getCreditLoc() {
        return creditLoc;
    }

    public long getFid() {
        return fid;
    }

    public boolean isNonAssgrp() {
        return nonAssgrp;
    }
}
