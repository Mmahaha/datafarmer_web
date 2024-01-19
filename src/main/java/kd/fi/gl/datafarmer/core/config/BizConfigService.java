package kd.fi.gl.datafarmer.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BizConfigService {

    private static Logger log = LoggerFactory.getLogger(BizConfigService.class);

    private static List<BizConfigVO> bizConfigCache = null;

    private static DBConfigVO dbConfig = null;

    private static Integer _parallelSize = null;

    private static Boolean _isDryRun = null;

    private static Boolean _justValidateConfig = null;

    private static Map<String, Boolean> _pluginMap = null;

    private static Boolean _isVoucherIrrigateEnable = null;

    private static Boolean _isRebuildIndexEnable = null;

    private static Boolean _isFixSumBalanceEnable = null;

    private static Boolean _isFixCashFlowEnable = null;

    private static Boolean _isFixVoucherCountEnable = null;

    private static Boolean _isFixEquityBalanceEnable = null;

    public static synchronized List<BizConfigVO> getBizConfig() {
        if (null == bizConfigCache) {
            List<Map<String, Object>> configList = (List<Map<String, Object>>) ConfigParser.getKey("datatask");
            bizConfigCache = configList.stream()
                    .filter(x -> null != x)
                    .map(x -> {
                        BizConfigVO p = BizConfigVO.loadFromMap(x);
                        log.info("biz config item: {}", p);
                        return p;
                    }).collect(Collectors.toList());
        }

        return bizConfigCache;
    }

    public static synchronized int getParallelSize() {
        if (null == _parallelSize) {
            _parallelSize = (Integer) ConfigParser.getKey("parallel");
        }

        return _parallelSize;
    }


    public static synchronized DBConfigVO getDatasourceConfig() {
        if (null == dbConfig) {
            Map<String, Object> config = (Map<String, Object>) ConfigParser.getKey("datasource");
            dbConfig = DBConfigVO.loadFromMap(config);
            log.info("datasource config info: {}", dbConfig);
        }

        return dbConfig;
    }

    public static synchronized Boolean isDryRun() {
        if (null == _isDryRun) {
            Boolean isDryRun = (Boolean) ConfigParser.getKey("isDryRun");
            _isDryRun = null == isDryRun ? false : isDryRun;
        }
        return _isDryRun;
    }

    /**
     * 是否仅验证配置的正确性，需要显示地指定为false才会真正执行任务。
     * 执行前务必先打开此参数先行验证。
     *
     * @return true -> 仅校验配置的正确性，不真正执行任务。<b>默认返回true</b>
     */
    public static synchronized Boolean isJustValidateConfig() {
        if (null == _justValidateConfig) {
            Boolean justValidateConfig = (Boolean) ConfigParser.getKey("justValidateConfig");
            _justValidateConfig = null == justValidateConfig ? true : justValidateConfig;
        }
        return _justValidateConfig;
    }

    public static synchronized Map<String, Boolean> getPluginMap() {
        if (null == _pluginMap) {
            _pluginMap = (Map<String, Boolean>) ConfigParser.getKey("plugins");
        }
        return _pluginMap;
    }

    public static synchronized Boolean isVoucherIrrigateEnable() {
        if (null == _isVoucherIrrigateEnable) {
            _isVoucherIrrigateEnable = getPluginMap() != null && getPluginMap().get("voucher irrigate");
        }
        return _isVoucherIrrigateEnable;
    }

    public static synchronized Boolean isRebuildIndexEnable() {
        if (null == _isRebuildIndexEnable) {
            _isRebuildIndexEnable = getPluginMap() != null && getPluginMap().get("rebuild index");
        }
        return _isRebuildIndexEnable;
    }

    public static synchronized Boolean isFixSumBalanceEnable() {
        if (null == _isFixSumBalanceEnable) {
            _isFixSumBalanceEnable = getPluginMap() != null && getPluginMap().get("fix sum balance");
        }
        return _isFixSumBalanceEnable;
    }

    public static synchronized Boolean isFixCashFlowEnable() {
        if (null == _isFixCashFlowEnable) {
            _isFixCashFlowEnable = getPluginMap() != null && getPluginMap().get("fix cash flow");
        }
        return _isFixCashFlowEnable;
    }


    public static synchronized Boolean isFixVoucherCountEnable() {
        if (null == _isFixVoucherCountEnable) {
            _isFixVoucherCountEnable = getPluginMap() != null && getPluginMap().get("fix voucher count");
        }
        return _isFixVoucherCountEnable;
    }

    public static synchronized Boolean isFixEquityBalanceEnable() {
        if (null == _isFixEquityBalanceEnable) {
            _isFixEquityBalanceEnable = getPluginMap() != null && getPluginMap().get("fix equity balance");
        }
        return _isFixEquityBalanceEnable;
    }

    public static void main(String[] args) throws Exception {
        BizConfigService.getBizConfig();
    }

}
