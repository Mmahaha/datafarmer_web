package kd.fi.gl.datafarmer.core.util;


import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AccountTypeService {

    /**
     * 资产	0
     * 负债	1
     * 权益	2
     * 成本	3
     * 损益	4
     */
    public static final String ASSETS = "0";
    public static final String DEBT = "1";
    public static final String EQUITY = "2";
    public static final String COST = "3";
    public static final String PL = "4";


    private static final Map<Long, Map<String, List<Long>>> ACCOUNT_TYPE_MAP;

    static {
        ACCOUNT_TYPE_MAP = new HashMap<>(4);
        DB.getFiJdbcTemplate().query("select faccounttableid,faccounttype,fid from T_bd_accounttype " +
                "where fisleaf = '1'", (RowCallbackHandler)  rs -> {
            long accountTableId = rs.getLong(1);
            Map<String, List<Long>> typeIdsMap = ACCOUNT_TYPE_MAP.computeIfAbsent(accountTableId, k -> new HashMap<>(2));
            String type = rs.getString(2);
            List<Long> typeIds = typeIdsMap.computeIfAbsent(type, k -> new ArrayList<>(10));
            typeIds.add(rs.getLong(3));
        });
    }


    /**
     * 获取科目表下的科目类型ID集合
     *
     * @param accountTableId 科目表ID
     * @param types          科目属性
     * @return 科目类型ID集合
     */
    public static List<Long> getAccountTypeIds(long accountTableId, String... types) {
        Map<String, List<Long>> typeIdMap = ACCOUNT_TYPE_MAP.get(accountTableId);
        if (typeIdMap == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(types).flatMap(t -> typeIdMap.getOrDefault(t, Collections.emptyList()).stream())
                .collect(Collectors.toList());
    }

}
