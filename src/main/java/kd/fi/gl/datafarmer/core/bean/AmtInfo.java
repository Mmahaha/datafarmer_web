package kd.fi.gl.datafarmer.core.bean;

public class AmtInfo {
    private final int oriAmt;
    private final int rate;
    private final int locAmt;

    public AmtInfo(int oriAmt, int rate, int locAmt) {
        this.oriAmt = oriAmt;
        this.rate = rate;
        this.locAmt = locAmt;
    }

    public int getOriAmt() {
        return oriAmt;
    }

    public int getRate() {
        return rate;
    }

    public int getLocAmt() {
        return locAmt;
    }
}
