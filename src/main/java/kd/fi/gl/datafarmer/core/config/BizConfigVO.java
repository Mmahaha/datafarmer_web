package kd.fi.gl.datafarmer.core.config;

import java.util.Map;

public class BizConfigVO {

    // 是否启用该配置项
    private boolean enable;

    private int distinctSign;

    private String orgSuffix;

    private String orgLimit;

    // 分录的原币币别
    private long currency;

    // 头行比（双数）: 凭证头有多少数量的分录
    private int entryRatio;

    // 重复度：每条CCID重复的数量
    private int repetition;

    //开始期间编码
    private String startPeriodNumber;

    //结束期间编码
    private String endPeriodNumber;

    private boolean containsCash;

    private String cashAccountLimit;

    private String mainCfItemLimit;

    private String suppCfItemLimit;

    private String accountType;

    private String accountLimit;

    private long hgIdStart;

    private long hgIdEnd;

    private String hgIdLimit;

    private BizConfigVO() {
        //禁止从外面构造对象
    }

    public static BizConfigVO loadFromMap(Map<String, Object> props) {
        BizConfigVO configVO = new BizConfigVO();
        configVO.enable = Boolean.parseBoolean(_checkValue(props, "enable", true).toString());
        configVO.distinctSign = Integer.parseInt(_checkValue(props, "distinctSign", true).toString());
        configVO.orgSuffix = (_checkValue(props, "orgSuffix", true).toString());
        configVO.orgLimit = (_checkValue(props, "orgLimit", true).toString());
        configVO.currency = Long.parseLong(_checkValue(props, "currency", true).toString());
        configVO.entryRatio = Integer.parseInt(_checkValue(props, "entryRatio", true).toString());
        configVO.repetition = Integer.parseInt(_checkValue(props, "repetition", true).toString());
        configVO.startPeriodNumber = (_checkValue(props, "startPeriodNumber", true).toString());
        configVO.endPeriodNumber = (_checkValue(props, "endPeriodNumber", true).toString());
        configVO.containsCash = Boolean.parseBoolean(_checkValue(props, "containsCash", true).toString());
        configVO.cashAccountLimit = String.valueOf(_checkValue(props, "cashAccountLimit", false));
        configVO.mainCfItemLimit = String.valueOf(_checkValue(props, "mainCfItemLimit", false));
        configVO.suppCfItemLimit = String.valueOf(_checkValue(props, "suppCfItemLimit", false));
        configVO.accountType = (_checkValue(props, "accountType", true).toString());
        configVO.accountLimit = (_checkValue(props, "accountLimit", true).toString());
        configVO.hgIdStart = Long.parseLong(_checkValue(props, "hgIdStart", true).toString());
        configVO.hgIdEnd = Long.parseLong(_checkValue(props, "hgIdEnd", true).toString());
        configVO.hgIdLimit = (_checkValue(props, "hgIdLimit", true).toString());
        return configVO;
    }

    private static Object _checkValue(Map<String, Object> props, String key, boolean isRequired) {
        Object v = props.get(key);
        if (isRequired && null == v) {
            throw new RuntimeException("config item:" + key + " is required, must specify.");
        }
        return v;
    }

    public boolean isEnable() {
        return enable;
    }

    public String getOrgSuffix() {
        return orgSuffix;
    }

    public String getOrgLimit() {
        return orgLimit;
    }

    public long getCurrency() {
        return currency;
    }

    public int getEntryRatio() {
        return entryRatio;
    }

    public int getRepetition() {
        return repetition;
    }

    public String getStartPeriodNumber() {
        return startPeriodNumber;
    }

    public String getEndPeriodNumber() {
        return endPeriodNumber;
    }

    public boolean isContainsCash() {
        return containsCash;
    }

    public String getCashAccountLimit() {
        return cashAccountLimit;
    }

    public String getMainCfItemLimit() {
        return mainCfItemLimit;
    }

    public String getSuppCfItemLimit() {
        return suppCfItemLimit;
    }

    public String getAccountType() {
        return accountType;
    }

    public String getAccountLimit() {
        return accountLimit;
    }

    public long getHgIdStart() {
        return hgIdStart;
    }

    public long getHgIdEnd() {
        return hgIdEnd;
    }

    public String getHgIdLimit() {
        return hgIdLimit;
    }

    public int getDistinctSign() {
        return distinctSign;
    }

    @Override
    public String toString() {
        return "BizConfigVO{" +
                "enable=" + enable +
                ", distinctSign=" + distinctSign +
                ", orgSuffix='" + orgSuffix + '\'' +
                ", orgLimit='" + orgLimit + '\'' +
                ", currency=" + currency +
                ", entryRatio=" + entryRatio +
                ", repetition=" + repetition +
                ", startPeriodNumber=" + startPeriodNumber +
                ", endPeriodNumber=" + endPeriodNumber +
                ", containsCash=" + containsCash +
                ", cashAccountLimit='" + cashAccountLimit + '\'' +
                ", mainCfItemLimit='" + mainCfItemLimit + '\'' +
                ", suppCfItemLimit='" + suppCfItemLimit + '\'' +
                ", accountType='" + accountType + '\'' +
                ", accountLimit='" + accountLimit + '\'' +
                ", hgIdStart=" + hgIdStart +
                ", hgIdEnd=" + hgIdEnd +
                ", hgIdLimit='" + hgIdLimit + '\'' +
                '}';
    }
}
