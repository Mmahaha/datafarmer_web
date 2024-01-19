package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.sharding.BalanceShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class FixEquityBalanceTask implements Callable<Integer> {

    private final long orgId;
    private final long shardingIndex;

    private static final Logger logger = LoggerFactory.getLogger(FixEquityBalanceTask.class);

    private static final String SQL_INSERT = "insert into t_gl_balance$%s(fid, fperiodid, fendperiodid, faccounttableid," +
            " faccountid, fcurrencyid, fbeginfor, fbeginlocal, fdebitfor, fdebitlocal, fcreditfor, fcreditlocal," +
            " fyeardebitfor, fyeardebitlocal, fyearcreditfor, fyearcreditlocal, fendfor, fendlocal, fcount, forgid, fbooktypeid)" +
            "select accsum.fid, accsum.fperiodid, accsum.fendperiodid, accsum.faccounttableid, accsum.faccountid, accsum.fcurrencyid, accsum.fbeginfor," +
            "accsum.fbeginlocal, accsum.fdebitfor, accsum.fdebitlocal, accsum.fcreditfor, accsum.fcreditlocal, accsum.fyeardebitfor, accsum.fyeardebitlocal," +
            "accsum.fyearcreditfor, accsum.fyearcreditlocal, accsum.fendfor, accsum.fendlocal, accsum.fcount, accsum.forgid, accsum.fbooktypeid " +
            "from t_gl_balance_accsum accsum inner join t_bd_account a on accsum.faccountid = a.fid where a.faccounttypeid = 1795136664729817088 and accsum.forgid = %s";

    public FixEquityBalanceTask(long orgId) {
        this.orgId = orgId;
        this.shardingIndex = BalanceShardingService.getShardingIndex(orgId);
    }

    @Override
    public Integer call() throws Exception {
        String sql = String.format(SQL_INSERT, shardingIndex, orgId);
        int update = DB.getFiJdbcTemplate().update(sql);
        logger.info("将权益类科目的汇总余额同步至科目余额成功，条数：{}", update);
        return null;
    }

}
