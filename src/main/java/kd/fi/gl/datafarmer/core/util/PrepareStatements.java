package kd.fi.gl.datafarmer.core.util;

import lombok.SneakyThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/30
 */
public class PrepareStatements {

    @SneakyThrows
    public static PreparedStatement cursor(Connection connection, String sql) {
        connection.setAutoCommit(false);
        return connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
    }

}
