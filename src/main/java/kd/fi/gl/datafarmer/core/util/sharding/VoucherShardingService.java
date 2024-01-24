package kd.fi.gl.datafarmer.core.util.sharding;

import kd.fi.gl.datafarmer.core.util.DB;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.Map;

public class VoucherShardingService {

    private static final Map<String, Integer> MAPPER; // 1#120230010 -> 10
    private static final Map<Long, Integer> extraModMap = new HashMap<>(8);
    private static final int mod;

    static {
        MAPPER = new HashMap<>(144);
        DB.getFiJdbcTemplate().query("select * from t_gl_voucher$map",
                (RowCallbackHandler)  rs -> MAPPER.put(rs.getString("fkey"), rs.getInt("findex")));
        String param = DB.getSysJdbcTemplate().query("select fstrategyparams from t_cbs_shard_config where fentitynumber = 'gl_voucher'",
                rs -> {
                    boolean hasNext = rs.next();
                    Assert.isTrue(hasNext, "未查询到凭证的分片配置");
                    return rs.getString(1);
                });
        Map<String, String> paramMap = new HashMap<>();
        String[] lines = param.split("\n");
        for (String line : lines) {
            String[] parts = line.split("=");
            if (parts.length == 2) {
                paramMap.put(parts[0], parts[1]);
            }
        }
        mod = Integer.parseInt(paramMap.get("p1.valueMapper.mod"));
        String extraModParam = paramMap.get("p1.valueMapper.mod.extra");
        if (extraModParam != null) {
            extraModParam = extraModParam.replaceAll("\n", "").replaceAll("\r\n", "");
            for (String segment : extraModParam.split(";")) {
                String[] parts = segment.split(":");
                for (String orgId : parts[0].split("\\|")) {
                    extraModMap.put(Long.parseLong(orgId), Integer.parseInt(parts[1]));
                }
            }
        }
    }

    public static int getShardingIndex(long orgId, long periodId) {
        String key = String.join("#", String.valueOf(getMod(orgId)), String.valueOf(periodId));
        Integer shardingIndex = MAPPER.get(key);
        if (shardingIndex == null) {
            throw new IllegalArgumentException(String.format("无法找到映射的分片,orgId=%s,periodId=%s,key=%s", orgId, periodId, key));
        }
        return shardingIndex;
    }


    private static int getMod(long orgId) {
        if (extraModMap.containsKey(orgId)) {
            return extraModMap.get(orgId);
        }
        String orgIdStr = String.valueOf(orgId);
        int hashCode = orgIdStr.hashCode();
        if (hashCode == Integer.MIN_VALUE) {
            hashCode = Integer.MAX_VALUE;
        }
        return Math.abs(hashCode) % mod;
    }



}
