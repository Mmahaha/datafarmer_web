package kd.fi.gl.datafarmer.core.util.helper;

import kd.fi.gl.datafarmer.core.util.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DDLSqlHelper {

    private static final Logger logger = LoggerFactory.getLogger(DDLSqlHelper.class);
    private static final String ADD_PRIMARY_KEY_SQL = "ALTER TABLE %s ADD PRIMARY KEY (%s);";
    private static final String CREATE_INDEX_SQL = "CREATE %s INDEX %s ON %s USING btree (%s);";


    public int createIndex(boolean unique, String indexName, String tableName, String columns) {
        long start = System.currentTimeMillis();
        String sql = String.format(CREATE_INDEX_SQL, unique ? "UNIQUE" : "", indexName, tableName, columns);
        try {
            int result = DB.getFiJdbcTemplate().update(sql);
            logger.info("[ddl]:重建表{}索引{}成功,耗时{}s", tableName, indexName, (System.currentTimeMillis() - start) / 1000);
            return result;
        } catch (Exception e) {
            // 可能是已存在之类的
            logger.warn("[ddl]:重建表{}索引{}失败:{},ddl:{}", tableName, indexName, e.getMessage(), sql);
            return 0;
        }
    }

    public int addPrimaryKey(String tableName, String column) {
        long start = System.currentTimeMillis();
        String sql = String.format(ADD_PRIMARY_KEY_SQL, tableName, column);
        try {
            int result = DB.getFiJdbcTemplate().update(sql);
            logger.info("[ddl]:添加表{}主键{}成功,耗时{}s", tableName, column, (System.currentTimeMillis() - start) / 1000);
            return result;
        } catch (Exception e) {
            // 可能是已存在之类的
            logger.warn("[ddl]:添加表{}主键{}失败:{},ddl:{}", tableName, column, e.getMessage(), sql);
            return 0;
        }
    }

}
