package kd.fi.gl.datafarmer.core.util;

import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.core.util.helper.CopyHelper;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.SQLException;

@Component
@Slf4j
public class DB {

    private static JdbcTemplateContainer container;

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

    @Autowired
    public void setContainer(JdbcTemplateContainer container) {
        log.info("setContainer is invoked.");
        DB.container = container;
    }


}
