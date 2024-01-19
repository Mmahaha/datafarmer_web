package kd.fi.gl.datafarmer.core.util;

import com.alibaba.druid.pool.DruidDataSource;
import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.core.config.BizConfigService;
import kd.fi.gl.datafarmer.core.config.DBConfigVO;
import kd.fi.gl.datafarmer.core.util.helper.CopyHelper;
import kd.fi.gl.datafarmer.core.util.helper.CopyHelperMock;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelperMock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.SQLException;

@Component
public class DB {

    private static JdbcTemplateContainer container;

    @Autowired
    public void setContainer(JdbcTemplateContainer container) {
        DB.container = container;
    }

    public static CopyHelper getCopyHelper() throws SQLException {
        return new CopyHelper(container.getFiConnection());
    }

    public static DDLSqlHelper getDDLSqlHelper() {
        return new DDLSqlHelper();
    }

    public static JdbcTemplate getFiJdbcTemplate() {
        return container.getFi();
    }

    public static JdbcTemplate getSysJdbcTemplate() {
        return container.getSys();
    }


}
