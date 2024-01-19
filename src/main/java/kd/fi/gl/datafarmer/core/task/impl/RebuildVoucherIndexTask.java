package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import kd.fi.gl.datafarmer.core.util.sharding.VoucherShardingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.Callable;

/**
 * 凭证索引重建任务，一个任务对应一个分片索引
 */
public class RebuildVoucherIndexTask implements Callable<Integer> {

    private static final Logger logger = LoggerFactory.getLogger(RebuildVoucherIndexTask.class);
    private final int shardingIndex;

    public RebuildVoucherIndexTask(long orgId, long periodId) {
        this.shardingIndex = VoucherShardingService.getShardingIndex(orgId, periodId);
    }

    @Override
    public Integer call() {
        DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
        return callInternal(ddlSqlHelper);
    }

    private Integer callInternal(DDLSqlHelper ddlSqlHelper) {
        String voucherHeadTable = "t_gl_voucher$" + shardingIndex;
        String voucherEntryTable = "t_gl_voucherentry$" + shardingIndex;
        int result = 0;
        result += ddlSqlHelper.addPrimaryKey(voucherHeadTable, "fid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vch_1$" + shardingIndex, voucherHeadTable, "forgid, fperiodid, fnumber, fid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vch_bookdate$" + shardingIndex, voucherHeadTable, "forgid, fbookeddate, fnumber, fid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vch_iop$" + shardingIndex, voucherHeadTable, "fispost, forgid, fbooktypeid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vchnum$" + shardingIndex, voucherHeadTable, "fnumber, forgid, fperiodid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vchsourcetype$" + shardingIndex, voucherHeadTable, "fsourcetype, forgid, fperiodid");

        result += ddlSqlHelper.addPrimaryKey(voucherEntryTable, "fentryid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_voucherentry$" + shardingIndex, voucherEntryTable, "fid, fseq");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vchenorg$" + shardingIndex, voucherEntryTable, "forgid, fperiodid, faccountid, fassgrpid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vchenassgrp$" + shardingIndex, voucherEntryTable, "fassgrpid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vch_maincf$" + shardingIndex, voucherEntryTable, "fmaincfitemid");
        result += ddlSqlHelper.createIndex(false, "idx_gl_vch_suppcf$" + shardingIndex, voucherEntryTable, "fsuppcfitemid");
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RebuildVoucherIndexTask that = (RebuildVoucherIndexTask) o;
        return shardingIndex == that.shardingIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardingIndex);
    }
}
