package kd.fi.gl.datafarmer.core.util.sharding;

import kd.fi.gl.datafarmer.core.util.DB;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.HashMap;
import java.util.Map;

public class VoucherShardingService {

    private static final Map<String, Integer> MAPPER; // 1#120230010 -> 10

    static {
        MAPPER = new HashMap<>(144);
        DB.getFiJdbcTemplate().query("select * from t_gl_voucher$map",
                (RowCallbackHandler)  rs -> MAPPER.put(rs.getString("fkey"), rs.getInt("findex")));
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
        String orgIdStr = String.valueOf(orgId);
        int hashCode = orgIdStr.hashCode();
        if (hashCode == Integer.MIN_VALUE) {
            hashCode = Integer.MAX_VALUE;
        }
        return Math.abs(hashCode) % 4;
    }



}
