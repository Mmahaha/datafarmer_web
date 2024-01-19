package kd.fi.gl.datafarmer.core.util;

import kd.fi.gl.datafarmer.core.config.BizConfigVO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CfItemBuilder {
    private final List<Long> mainCfItemIds;
    private final List<Long> suppCfItemIds;


    @SuppressWarnings("unchecked")
    public CfItemBuilder(BizConfigVO configVO) {
        if (!configVO.isContainsCash()) {
            mainCfItemIds = Collections.EMPTY_LIST;
            suppCfItemIds = Collections.EMPTY_LIST;
            return;
        }
        // 主表
        int mainCfItemLimit = FastStringUtils.extractLimitFromLimitOffsetStr(configVO.getMainCfItemLimit());
        mainCfItemIds = new ArrayList<>(mainCfItemLimit);
        DB.getFiJdbcTemplate().query("select fid from t_gl_cashflowitem where ftype = '1' and FISDEALACTIVITY = '1' and fisleaf = '1' " + configVO.getMainCfItemLimit(),
                rs -> {mainCfItemIds.add(rs.getLong("fid"));});
        if (mainCfItemIds.size() != mainCfItemLimit) {
            throw new IllegalArgumentException("主表项目数量不足，需" + mainCfItemLimit + "，实际" + mainCfItemIds.size() + "，config=" + configVO);
        }
        // 附表
        int suppCfItemLimit = FastStringUtils.extractLimitFromLimitOffsetStr(configVO.getSuppCfItemLimit());
        suppCfItemIds = new ArrayList<>(suppCfItemLimit);
        DB.getFiJdbcTemplate().query("select fid from t_gl_cashflowitem where ftype = '3' and fisleaf = '1' " + configVO.getSuppCfItemLimit(),
                rs -> {suppCfItemIds.add(rs.getLong("fid"));});
        if (suppCfItemIds.size() != suppCfItemLimit) {
            throw new IllegalArgumentException("附表项目数量不足，需" + suppCfItemLimit + "，实际" + suppCfItemIds.size() + "，config=" + configVO);
        }
    }

    public List<Long> getMainCfItemIds() {
        return mainCfItemIds;
    }

    public List<Long> getSuppCfItemIds() {
        return suppCfItemIds;
    }
}
