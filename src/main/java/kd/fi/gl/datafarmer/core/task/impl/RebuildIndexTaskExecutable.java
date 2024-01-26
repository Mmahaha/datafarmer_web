package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.common.exception.impl.ParseConfigException;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/19
 */
@Data
@Slf4j
@SuperBuilder
@NoArgsConstructor
public class RebuildIndexTaskExecutable implements TaskExecutable {

    private boolean rebuildVoucher;
    private boolean rebuildBalance;
    private boolean rebuildCashFlow;

    @Override
    public boolean supportSplit() {
        return true;
    }

    @Override
    public List<? extends TaskExecutable> split() {
        List<TaskExecutable> result = new ArrayList<>(50);
        if (rebuildBalance) {
            result.add(new RebuildBalanceIndexTaskExecutable());
        }
        if (rebuildCashFlow) {
            result.add(new RebuildCashFlowIndexTaskExecutable());
        }
        if (rebuildVoucher) {
            List<String> voucherIndexes = DB.getFiJdbcTemplate()
                    .queryForList("select indexdef from pg_indexes where tablename='t_gl_voucher$ori'", String.class);
            List<String> entryIndexes = DB.getFiJdbcTemplate()
                    .queryForList("select indexdef from pg_indexes where tablename='t_gl_voucherentry$ori'", String.class);
            Assert.isTrue(!voucherIndexes.isEmpty() && !entryIndexes.isEmpty(),
                    "未检测到t_gl_voucher$ori/t_gl_voucherentry$ori索引，无法重建凭证索引");
            for (Integer index : DB.getFiJdbcTemplate().queryForList("select findex from t_gl_voucher$map", Integer.class)) {
                result.add(new RebuildVoucherIndexTaskExecutable(index, voucherIndexes, entryIndexes));
            }
        }
        return result;
    }

    @EqualsAndHashCode(callSuper = true)
    @Slf4j
    @Data
    private static class RebuildVoucherIndexTaskExecutable extends RebuildIndexTaskExecutable {

        private final int index;
        private final List<String> oriVoucherIndexes;
        private final List<String> oriEntryIndexes;


        @Override
        public boolean supportSplit() {
            return false;
        }

        @Override
        public void execute() {
            DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
            ddlSqlHelper.addPrimaryKey("t_gl_voucher$" + index, "fid");
            ddlSqlHelper.addPrimaryKey("t_gl_voucherentry$" + index, "fentryid");
            for (String oriVoucherIndex : oriVoucherIndexes) {
                if (oriVoucherIndex.contains("_pkey")) {
                    continue;
                }
                ddlSqlHelper.executeWithoutException(oriVoucherIndex.replaceAll("\\$ori", Matcher.quoteReplacement("$" + index)));
            }
            for (String oriVoucherIndex : oriEntryIndexes) {
                if (oriVoucherIndex.contains("_pkey")) {
                    continue;
                }
                ddlSqlHelper.executeWithoutException(oriVoucherIndex.replaceAll("\\$ori", Matcher.quoteReplacement("$" + index)));
            }
        }
    }

    private static class RebuildBalanceIndexTaskExecutable extends RebuildIndexTaskExecutable {
        private static final String TABLE_BALANCE = "t_gl_balance";
        @Override
        public boolean supportSplit() {
            return false;
        }

        @Override
        public void execute() {
            DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
            ddlSqlHelper.addPrimaryKey(TABLE_BALANCE, "fid");
            ddlSqlHelper.createIndex(false, "idx_gl_balance2", TABLE_BALANCE, "forgid, fendperiodid, fperiodid, faccountid");
            ddlSqlHelper.createIndex(false, "idx_gl_balance_aop", TABLE_BALANCE, "faccountid, forgid, fperiodid");
            ddlSqlHelper.createIndex(false, "idx_gl_balance_assgrp", TABLE_BALANCE, "fassgrpid");
            ddlSqlHelper.createIndex(true, "idx_gl_balance_1", TABLE_BALANCE, "forgid, fbooktypeid, faccounttableid, fendperiodid, faccountid, fassgrpid, fcurrencyid, fmeasureunitid, fcomassist1id, fcomassist2id");
        }
    }

    private static class RebuildCashFlowIndexTaskExecutable extends RebuildIndexTaskExecutable {
        private static final String TABLE_CASHFLOW = "t_gl_cashflow";
        @Override
        public boolean supportSplit() {
            return false;
        }

        @Override
        public void execute() {
            DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
            ddlSqlHelper.addPrimaryKey(TABLE_CASHFLOW, "fid");
            ddlSqlHelper.createIndex(false, "idx_gl_cashflow_cforg", TABLE_CASHFLOW, "fcfitemid, forgid");
            ddlSqlHelper.createIndex(false, "idx_gl_cfbip", TABLE_CASHFLOW, "forgid, fbooktypeid, fendperiodid, fcfitemid, fassgrpid, fcurrencyid");
        }
    }

}
