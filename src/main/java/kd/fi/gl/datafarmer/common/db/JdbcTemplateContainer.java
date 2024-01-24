package kd.fi.gl.datafarmer.common.db;

import com.alibaba.druid.pool.DruidDataSource;
import kd.fi.gl.datafarmer.common.exception.impl.DatabaseNotInitializedException;
import kd.fi.gl.datafarmer.model.DBConfig;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Description: 延迟注入数据库配置信息
 *
 * @author ysj
 * @date 2024/1/15
 */

@Component
public class JdbcTemplateContainer {

    private DruidDataSource fiDataSource;
    private JdbcTemplate fiJdbcTemplate;
    private DruidDataSource sysDataSource;
    private JdbcTemplate sysJdbcTemplate;
    private DBConfig dbConfig;
    private boolean isInitialized = false;


    public JdbcTemplate getFi() {
        checkInit();
        return fiJdbcTemplate;
    }

    public JdbcTemplate getSys() {
        checkInit();
        return sysJdbcTemplate;
    }

    public Connection getFiConnection() throws SQLException {
        checkInit();
        return fiDataSource.getConnection();
    }

    public Connection getSysConnection() throws SQLException {
        checkInit();
        return sysDataSource.getConnection();
    }



    public synchronized void init(DBConfig dbConfig) {
        if (isInitialized) {
            throw new IllegalStateException("duplicate init database!");
        }
        DruidDataSource fiDataSource = new DruidDataSource();
        fiDataSource.setUrl(String.format("jdbc:postgresql://%s:%s/%s?defaultRowFetchSize=5000&useCursorFetch=true",
                dbConfig.getHost(), dbConfig.getPort(), dbConfig.getFiDatabase()));
        fiDataSource.setUsername(dbConfig.getUser());
        fiDataSource.setPassword(dbConfig.getPassword());
        fiDataSource.setInitialSize(5);
        fiDataSource.setMaxActive(100);
        fiDataSource.setMaxWait(60000);
        fiDataSource.setSocketTimeout(Integer.MAX_VALUE);
        fiDataSource.setBreakAfterAcquireFailure(true);
        JdbcTemplate fiJdbcTemplate = new JdbcTemplate(fiDataSource);
        fiJdbcTemplate.queryForObject("SELECT 1", Integer.class);

        DruidDataSource sysDataSource = new DruidDataSource();
        sysDataSource.setUrl(String.format("jdbc:postgresql://%s:%s/%s?defaultRowFetchSize=5000&useCursorFetch=true",
                dbConfig.getHost(), dbConfig.getPort(), dbConfig.getSysDatabase()));
        sysDataSource.setUsername(dbConfig.getUser());
        sysDataSource.setPassword(dbConfig.getPassword());
        sysDataSource.setInitialSize(5);
        sysDataSource.setMaxActive(50);
        sysDataSource.setMaxWait(60000);
        sysDataSource.setSocketTimeout(Integer.MAX_VALUE);
        sysDataSource.setBreakAfterAcquireFailure(true);
        JdbcTemplate sysJdbcTemplate = new JdbcTemplate(sysDataSource);
        sysJdbcTemplate.queryForObject("SELECT 1", Integer.class);
        this.fiJdbcTemplate = fiJdbcTemplate;
        this.sysJdbcTemplate = sysJdbcTemplate;
        this.fiDataSource = fiDataSource;
        this.sysDataSource = sysDataSource;
        this.dbConfig = dbConfig;
        this.isInitialized = true;
    }

    public boolean isInitialized() {
        return isInitialized;
    }

    private void checkInit() {
        if (fiJdbcTemplate == null || sysJdbcTemplate == null) {
            throw new DatabaseNotInitializedException();
        }
    }

    public DBConfig getDbConfig() {
        return dbConfig;
    }
}
