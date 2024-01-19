package kd.fi.gl.datafarmer.core.util;

import kd.fi.gl.datafarmer.core.config.BizConfigVO;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

public class OrgBuilder {

    private static List<Long> allOrgIds;    // 5000个
    private final List<Long> orgIds;


    public OrgBuilder(BizConfigVO configVO) {
        this(configVO.getOrgSuffix(), configVO.getOrgLimit());
    }

    public OrgBuilder(String orgSuffix, String orgLimit) {
        int limit = FastStringUtils.extractLimitFromLimitOffsetStr(orgLimit);
        orgIds = new ArrayList<>(limit);
        DB.getSysJdbcTemplate().query("select fid from t_org_org where fnumber like ? order by fnumber " + orgLimit,
                new Object[]{"%" + orgSuffix}, new int[]{Types.VARCHAR}, rs -> {
                    orgIds.add(rs.getLong("fid"));
                });
        if (orgIds.size() != limit) {
            throw new IllegalArgumentException("核算组织数量不足，需" + limit + "，实际" + orgIds.size());
        }
    }

    public static List<Long> getAllOrgIds() {
        if (allOrgIds == null) {
            allOrgIds = new ArrayList<>(5000);
            allOrgIds.addAll(new OrgBuilder("A", "limit 1000").getOrgIds());
            allOrgIds.addAll(new OrgBuilder("B", "limit 4000").getOrgIds());
        }
        return allOrgIds;
    }

    public List<Long> getOrgIds() {
        return orgIds;
    }
}
