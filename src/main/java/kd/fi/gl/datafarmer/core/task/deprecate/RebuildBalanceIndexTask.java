package kd.fi.gl.datafarmer.core.task.deprecate;

import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import kd.fi.gl.datafarmer.core.util.sharding.BalanceShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * 余额索引重建任务
 */
public class RebuildBalanceIndexTask implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RebuildBalanceIndexTask.class);


    private static final String TABLE_SUM_BALANCE = "t_gl_balance_accsum";

    private final int shardingIndex;

    public RebuildBalanceIndexTask(long orgId) {
        this.shardingIndex = BalanceShardingService.getShardingIndex(orgId);
    }

    @Override
    public Integer call() throws Exception {
        try {
            return callInternal(DB.getDDLSqlHelper());
        } catch (Exception e) {
            logger.error("rebuild balance index error", e);
            throw e;
        }
    }

    private Integer callInternal(DDLSqlHelper ddlSqlHelper) {
        String balanceTable = "t_gl_balance$" + shardingIndex;
        int result = 0;
        result += ddlSqlHelper.addPrimaryKey(balanceTable, "fid");
        result += ddlSqlHelper.createIndex(true, "idx_gl_balance_1$" + shardingIndex, balanceTable, "forgid, fbooktypeid, faccounttableid, fendperiodid, faccountid, fassgrpid, fcurrencyid, fmeasureunitid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_balance2$" + shardingIndex, balanceTable, "forgid, fendperiodid, fperiodid, faccountid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_balance_aop$" + shardingIndex, balanceTable, "fassgrpid, forgid, fendperiodid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_balance_assgrp$" + shardingIndex, balanceTable, "fassgrpid");

        result += ddlSqlHelper.addPrimaryKey(TABLE_SUM_BALANCE, "fid");
        result += ddlSqlHelper.createIndex(true, "idx_gl_balance_accsum_1", TABLE_SUM_BALANCE, "forgid, fendperiodid, faccountid, fcurrencyid, fmeasureunitid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_balance_accsumacas", TABLE_SUM_BALANCE, "faccountid, fendperiodid");

        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RebuildBalanceIndexTask that = (RebuildBalanceIndexTask) o;
        return shardingIndex == that.shardingIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardingIndex);
    }
}
