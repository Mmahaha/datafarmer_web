package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.util.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * 凭证计数修复任务
 */
public class FixVoucherCountTask implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(FixVoucherCountTask.class);
    private static final String UPDATE_SQL = "update t_gl_vouchercount set fvouchercount = ? , fentrycount = ? where fid = ?";

    private static final String DELETE_SQL = "delete from t_gl_vouchercount where fid in (%s)";

    private final long orgId;

    public FixVoucherCountTask(long orgId) {
        this.orgId = orgId;
    }

    @Override
    public Integer call() throws Exception {
        return DB.getFiJdbcTemplate().query(con -> {
            PreparedStatement preparedStatement = con.prepareStatement("select fid,forgid,fbooktypeid,fperiodid,fbillstatus,fsourcetype," +
                    "fvouchercount,fentrycount,fischeck,fispost,fbookeddate from t_gl_vouchercount where forgid = " + orgId +
                    " order by forgid, fperiodid, fbookeddate", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
            con.setAutoCommit(false);
            return preparedStatement;
        }, resultSet -> {
            try {
                return dealResultSet(resultSet);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private int dealResultSet(ResultSet resultSet) throws Exception {
        String lastRowKey = "", curRowKey = "";
        List<Long> deleteFids = new ArrayList<>(10000);
        long headCount = 0, entryCount = 0;
        long lastFid = -1;
        List<Object[]> updateParams = new ArrayList<>(10000);
        boolean errorFlag = false;
        while (resultSet.next()) {
            curRowKey = buildRowKey(resultSet);
            if (!curRowKey.equals(lastRowKey)) {
                if (errorFlag) {
                    updateParams.add(new Object[]{headCount, entryCount, lastFid});
                }
                // reset
                errorFlag = false;
                headCount = resultSet.getLong("fvouchercount");
                entryCount = resultSet.getLong("fentrycount");
            } else {
                errorFlag = true;
                deleteFids.add(lastFid);
                headCount += resultSet.getLong("fvouchercount");
                entryCount += resultSet.getLong("fentrycount");
            }

            lastFid = resultSet.getLong("fid");
            lastRowKey = curRowKey;
        }
        // 最后一批
        if (errorFlag) {
            updateParams.add(new Object[]{headCount, entryCount, lastFid});
        }

        int result = 0;

        result += IntStream.of(DB.getFiJdbcTemplate().batchUpdate(UPDATE_SQL, updateParams)).sum();
        if (!deleteFids.isEmpty()) {
            result += DB.getFiJdbcTemplate().update(String.format(DELETE_SQL, deleteFids.stream().map(String::valueOf).collect(Collectors.joining(","))));
        }
        logger.info("修复凭证计数表成功，count={}", result);
        return result;
    }

    private String buildRowKey(ResultSet resultSet) throws Exception {
        return String.join("-",
                resultSet.getString("forgid"),
                resultSet.getString("fbookeddate"));
    }
}
