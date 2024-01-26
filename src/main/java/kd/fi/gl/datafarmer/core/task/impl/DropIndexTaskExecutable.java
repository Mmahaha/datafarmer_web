package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.helper.DDLSqlHelper;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Description: 删除索引
 *
 * @author ysj
 * @date 2024/1/26
 */
@Data
@Slf4j
@SuperBuilder
@NoArgsConstructor
public class DropIndexTaskExecutable implements TaskExecutable {

    private boolean dropVoucherIndex;
    private boolean dropBalanceIndex;
    private boolean dropCashFlowIndex;

    @Override
    public boolean supportSplit() {
        return true;
    }

    @Override
    public List<? extends TaskExecutable> split() {
        List<TaskExecutable> result = new ArrayList<>();
        if (dropBalanceIndex) {
            result.add(new DropBalanceIndexTaskExecutable());
        }
        if (dropCashFlowIndex) {
            result.add(new DropCashFlowIndexTaskExecutable());
        }
        if (dropVoucherIndex) {
            for (Integer index : DB.getFiJdbcTemplate().queryForList("select findex from t_gl_voucher$map", Integer.class)) {
                result.add(new DropVoucherIndexTaskExecutable(index));
            }
        }
        return result;
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    private static class DropVoucherIndexTaskExecutable extends DropIndexTaskExecutable {

        private final int index;

        @Override
        public boolean supportSplit() {
            return false;
        }

        @Override
        public void execute() {
            String headTableName = "t_gl_voucher$" + index;
            DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
            for (String index : DB.getFiJdbcTemplate()
                    .queryForList("select indexname from pg_indexes where tablename = ?", String.class, headTableName)) {
                if (index.contains("pkey")) {
                    ddlSqlHelper.dropConstraint(headTableName, index);
                } else {
                    ddlSqlHelper.dropIndex(index);
                }
            }
            String entryTableName = "t_gl_voucherentry$" + index;
            for (String index : DB.getFiJdbcTemplate()
                    .queryForList("select indexname from pg_indexes where tablename = ?", String.class, entryTableName)) {
                if (index.contains("pkey")) {
                    ddlSqlHelper.dropConstraint(entryTableName, index);
                } else {
                    ddlSqlHelper.dropIndex(index);
                }
            }
        }
    }

    private static class DropBalanceIndexTaskExecutable extends DropIndexTaskExecutable {
        private static final String TABLE_BALANCE = "t_gl_balance";

        @Override
        public boolean supportSplit() {
            return false;
        }

        @Override
        public void execute() {
            DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
            for (String index : DB.getFiJdbcTemplate()
                    .queryForList("select indexname from pg_indexes where tablename = ?", String.class, TABLE_BALANCE)) {
                if (index.contains("pkey")) {
                    ddlSqlHelper.dropConstraint(TABLE_BALANCE, index);
                } else {
                    ddlSqlHelper.dropIndex(index);
                }
            }
        }
    }


    private static class DropCashFlowIndexTaskExecutable extends DropIndexTaskExecutable {
        private static final String TABLE_CASHFLOW = "t_gl_cashflow";

        @Override
        public boolean supportSplit() {
            return false;
        }

        @Override
        public void execute() {
            DDLSqlHelper ddlSqlHelper = DB.getDDLSqlHelper();
            for (String index : DB.getFiJdbcTemplate()
                    .queryForList("select indexname from pg_indexes where tablename = ?", String.class, TABLE_CASHFLOW)) {
                if (index.contains("pkey")) {
                    ddlSqlHelper.dropConstraint(TABLE_CASHFLOW, index);
                } else {
                    ddlSqlHelper.dropIndex(index);
                }
            }
        }
    }

}
