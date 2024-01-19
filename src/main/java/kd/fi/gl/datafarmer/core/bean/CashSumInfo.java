package kd.fi.gl.datafarmer.core.bean;

/**
 * 现金流量合计
 */
public class CashSumInfo {

    private final long fid;

    private final long cfItemId;

    private long amount = 0;

    public CashSumInfo(long fid, long cfItemId) {
        this.fid = fid;
        this.cfItemId = cfItemId;
    }

    public void add(int amount) {
        this.amount += amount;
    }

    public long getFid() {
        return fid;
    }

    public long getCfItemId() {
        return cfItemId;
    }

    public long getAmount() {
        return amount;
    }
}
