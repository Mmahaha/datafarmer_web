package kd.fi.gl.datafarmer.core.bean;

import lombok.Data;

@Data
public class CashInfo {

    private final long mainCfItemId;
    private final long suppCfItemId;
    private final long mainCFAssgrpId;
    private final int cfAmount;    // 主附表金额

    public static final CashInfo ZERO = new CashInfo(0L, 0L, 0L, 0);

}
