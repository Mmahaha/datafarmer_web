package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.util.DB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.IntStream;

/**
 * 余额汇总表数据修复任务
 */
public class FixSumBalanceTask implements Callable<Integer> {

    public static final Long YEAR_PERIOD_L = 10000L;
    private static final Logger logger = LoggerFactory.getLogger(FixSumBalanceTask.class);
    private final long orgId;

    public FixSumBalanceTask(long orgId) {
        this.orgId = orgId;
    }

    @Override
    public Integer call() {
        DB.getFiJdbcTemplate().query(con -> {
                    PreparedStatement preparedStatement = con.prepareStatement(" select fid,forgid,fperiodid,fendperiodid,faccountid,fbeginfor,fdebitfor,fcurrencyid" +
                                    ",fcreditfor,fendfor,fbeginlocal,fdebitlocal,fcreditlocal,fendlocal,fyeardebitfor,fyeardebitlocal,fyearcreditfor,fyearcreditlocal " +
                                    " from t_gl_balance_accsum where forgid = " + orgId + " order by forgid, fcurrencyid, faccountid, fperiodid",
                            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
                    con.setAutoCommit(false);
                    return preparedStatement;
                },
                resultSet -> {
                    try {
                        BigDecimal endfor = BigDecimal.ZERO;
                        BigDecimal endlocal = BigDecimal.ZERO;
                        BigDecimal ydf = BigDecimal.ZERO;
                        BigDecimal ydl = BigDecimal.ZERO;
                        BigDecimal ycf = BigDecimal.ZERO;
                        BigDecimal ycl = BigDecimal.ZERO;
                        String updateSql = "update t_gl_balance_accsum set fbeginfor=?,fbeginlocal=?,fyeardebitfor=?,fyeardebitlocal=?,fyearcreditfor=?,fyearcreditlocal=?,fendfor=?,fendlocal=? where fid=?";
                        String updatePeriod = "update t_gl_balance_accsum set fendperiodid=? where fid=?";
                        List<Object[]> params = new ArrayList<>(100);
                        List<Object[]> periodParams = new ArrayList<>(100);
                        long lastPeriodId = 0L;
                        long lastFid = 0L;
                        String lastKey = "";
                        while (resultSet.next()) {
                            String key = String.join("-", resultSet.getString("forgid"), resultSet.getString("fcurrencyid"), resultSet.getString("faccountid"));
                            long periodId = resultSet.getLong("fperiodid");
                            if (!key.equals(lastKey)) {
                                endfor = BigDecimal.ZERO;
                                endlocal = BigDecimal.ZERO;
                                ydf = resultSet.getBigDecimal("fdebitfor");
                                ydl = resultSet.getBigDecimal("fdebitlocal");
                                ycf = resultSet.getBigDecimal("fcreditfor");
                                ycl = resultSet.getBigDecimal("fcreditlocal");
                            } else {
                                boolean isCurrYear = periodId / YEAR_PERIOD_L == lastPeriodId / YEAR_PERIOD_L;
                                if (isCurrYear) {
                                    ydf = resultSet.getBigDecimal("fdebitfor").add(ydf);
                                    ydl = resultSet.getBigDecimal("fdebitlocal").add(ydl);
                                    ycf = resultSet.getBigDecimal("fcreditfor").add(ycf);
                                    ycl = resultSet.getBigDecimal("fcreditlocal").add(ycl);
                                } else {
                                    ydf = resultSet.getBigDecimal("fdebitfor");
                                    ydl = resultSet.getBigDecimal("fdebitlocal");
                                    ycf = resultSet.getBigDecimal("fcreditfor");
                                    ycl = resultSet.getBigDecimal("fcreditlocal");
                                }
                                periodParams.add(new Object[]{periodId, lastFid});
                            }
                            lastFid = resultSet.getLong("fid");
                            lastPeriodId = periodId;
                            lastKey = key;
                            Object[] param = new Object[9];
                            param[0] = endfor;
                            param[1] = endlocal;
                            param[2] = ydf;
                            param[3] = ydl;
                            param[4] = ycf;
                            param[5] = ycl;
                            endfor = endfor.add(resultSet.getBigDecimal("fdebitfor")).subtract(resultSet.getBigDecimal("fcreditfor"));
                            endlocal = endlocal.add(resultSet.getBigDecimal("fdebitlocal")).subtract(resultSet.getBigDecimal("fcreditlocal"));
                            param[6] = endfor;
                            param[7] = endlocal;
                            param[8] = resultSet.getLong("fid");
                            params.add(param);
                            if (params.size() > 10000) {
                                int count = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(updateSql, params)).sum();
                                int periodCnt = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(updatePeriod, periodParams)).sum();
                                params.clear();
                                periodParams.clear();
                                logger.info("更新余额汇总表成功，条数：{} + {}", count, periodCnt);
                            }
                        }
                        if (!params.isEmpty()) {
                            int count = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(updateSql, params)).sum();
                            int periodCnt = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(updatePeriod, periodParams)).sum();
                            logger.info("更新余额汇总表成功，条数：{} + {}", count, periodCnt);
                        }
                        return null;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
        return null;
    }

}
