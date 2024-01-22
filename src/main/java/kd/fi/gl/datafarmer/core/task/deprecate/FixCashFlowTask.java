package kd.fi.gl.datafarmer.core.task.deprecate;


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
 * 现金流量修复，将结束期间改为正确的期间
 */
public class FixCashFlowTask implements Callable<Integer> {

    public static final Long YEAR_PERIOD_L = 10000L;
    private static final Logger logger = LoggerFactory.getLogger(FixCashFlowTask.class);
    private static final String SQL_UPDATE_AMOUNT = "update t_gl_cashflow set fyearamount = ? where fid = ?";
    private static final String SQL_UPDATE_PERIOD = "update t_gl_cashflow set fendperiodid = ? where fid = ?";
    private final long orgId;

    public FixCashFlowTask(long orgId) {
        this.orgId = orgId;
    }

    @Override
    public Integer call() {
        DB.getFiJdbcTemplate().query(con -> {
                    PreparedStatement preparedStatement = con.prepareStatement(" select * from t_gl_cashflow where forgid = " + orgId + " order by forgid, fcurrencyid, fcfitemid, fperiodid",
                            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
                    con.setAutoCommit(false);
                    return preparedStatement;
                },
                resultSet -> {
                    try {
                        BigDecimal yearAmount = BigDecimal.ZERO;
                        List<Object[]> params = new ArrayList<>(100);
                        List<Object[]> periodParams = new ArrayList<>(100);
                        long lastPeriodId = 0L;
                        long lastFid = 0L;
                        String lastKey = "";
                        while (resultSet.next()) {
                            String key = String.join("-", resultSet.getString("forgid"), resultSet.getString("fcurrencyid"), resultSet.getString("fcfitemid"));
                            long periodId = resultSet.getLong("fperiodid");
                            if (!key.equals(lastKey)) {
                                yearAmount = resultSet.getBigDecimal("famount");
                            } else {
                                boolean isCurrYear = periodId / YEAR_PERIOD_L == lastPeriodId / YEAR_PERIOD_L;
                                if (isCurrYear) {
                                    yearAmount = yearAmount.add(resultSet.getBigDecimal("famount"));
                                } else {
                                    yearAmount = resultSet.getBigDecimal("famount");
                                }
                                periodParams.add(new Object[]{periodId, lastFid});
                            }
                            lastFid = resultSet.getLong("fid");
                            lastPeriodId = periodId;
                            lastKey = key;
                            Object[] param = new Object[] {yearAmount, resultSet.getLong("fid")};
                            params.add(param);
                            if (params.size() > 10000) {
                                int count = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(SQL_UPDATE_AMOUNT, params)).sum();
                                int periodCnt = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(SQL_UPDATE_PERIOD, periodParams)).sum();
                                params.clear();
                                periodParams.clear();
                                logger.info("更新现金流量表成功，条数：{} + {}", count, periodCnt);
                            }
                        }
                        if (!params.isEmpty()) {
                            int count = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(SQL_UPDATE_AMOUNT, params)).sum();
                            int periodCnt = IntStream.of(DB.getFiJdbcTemplate().batchUpdate(SQL_UPDATE_PERIOD, periodParams)).sum();
                            logger.info("更新现金流量表成功，条数：{} + {}", count, periodCnt);
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
