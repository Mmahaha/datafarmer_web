package kd.fi.gl.datafarmer.core.bean;

public class CashInfo {

    private final long mainCfItemId;
    private final long suppCfItemId;
    private final int cfAmount;    // 主附表金额

    public static final CashInfo ZERO = new CashInfo(0L, 0L, 0);

    public CashInfo(long mainCfItemId, long suppCfItemId, int cfAmount) {
        this.mainCfItemId = mainCfItemId;
        this.suppCfItemId = suppCfItemId;
        this.cfAmount = cfAmount;
    }

    public long getMainCfItemId() {
        return mainCfItemId;
    }

    public long getSuppCfItemId() {
        return suppCfItemId;
    }

    public int getCfAmount() {
        return cfAmount;
    }
}
