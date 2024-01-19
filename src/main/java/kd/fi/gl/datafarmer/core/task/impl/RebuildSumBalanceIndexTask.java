package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class RebuildSumBalanceIndexTask implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RebuildSumBalanceIndexTask.class);
    private static final String TABLE_SUM_BALANCE = "t_gl_balance_accsum";

    @Override
    public Integer call() throws Exception {
        try {
            return callInternal(DB.getDDLSqlHelper());
        } catch (Exception e) {
            logger.error("rebuild sumbalance index error", e);
            throw e;
        }
    }

    private Integer callInternal(DDLSqlHelper ddlSqlHelper) {
        int result = 0;
        result += ddlSqlHelper.addPrimaryKey(TABLE_SUM_BALANCE, "fid");
        result += ddlSqlHelper.createIndex(true, "idx_gl_balance_accsum_1", TABLE_SUM_BALANCE, "forgid, fendperiodid, faccountid, fcurrencyid, fmeasureunitid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_balance_accsumacas", TABLE_SUM_BALANCE, "faccountid, fendperiodid");

        return result;
    }
}
