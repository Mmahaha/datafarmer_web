package kd.fi.gl.datafarmer.core.task.impl;

import kd.fi.gl.datafarmer.common.db.JdbcTemplateContainer;
import kd.fi.gl.datafarmer.common.exception.impl.ParseConfigException;
import kd.fi.gl.datafarmer.core.task.TaskExecutable;
import kd.fi.gl.datafarmer.core.util.BookService;
import kd.fi.gl.datafarmer.core.util.DB;
import kd.fi.gl.datafarmer.core.util.ID;
import kd.fi.gl.datafarmer.core.util.PeriodVOBuilder;
import kd.fi.gl.datafarmer.core.util.PrepareStatements;
import kd.fi.gl.datafarmer.core.util.RowsBuilder;
import kd.fi.gl.datafarmer.core.util.RowsWriter;
import kd.fi.gl.datafarmer.core.util.helper.CopyHelper;
import kd.fi.gl.datafarmer.core.util.sharding.VoucherShardingService;
import kd.fi.gl.datafarmer.model.DBConfig;
import lombok.AccessLevel;
import lombok.Cleanup;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.util.Assert;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/30
 */
@Data
@Slf4j
@SuperBuilder
@NoArgsConstructor
public class AccCurrentTaskExecutable implements TaskExecutable {


    private String startPeriodNumber;

    private String endPeriodNumber;

    /**
     * 组织范围
     */
    private String orgSelectSql;

    /**
     * 往来科目范围
     */
    private String accAccountSelectSql;

    /**
     * 是否写入往来初始化状态表
     */
    private boolean initState;

    @Override
    public boolean supportSplit() {
        return true;
    }

    /**
     * 按组织拆分任务
     */
    @Override
    public List<? extends TaskExecutable> split() {
        List<Long> orgIds = fetchBaseData(orgSelectSql, "org");
        List<TaskExecutable> result = new ArrayList<>();
        List<Long> accountIds = fetchBaseData(accAccountSelectSql, "account");
        List<PeriodVOBuilder.PeriodVO> periodVOList = new PeriodVOBuilder(1L, startPeriodNumber, endPeriodNumber).getPeriodVOList();
        if (initState) {
            result.add(InitReciStateTaskExecutable.builder()
                    .accountIds(accountIds)
                    .orgIds(orgIds)
                    .endInitPeriodId(periodVOList.get(0).getId())
                    .build());
        }
        for (Long orgId : orgIds) {
            for (PeriodVOBuilder.PeriodVO periodVO : periodVOList) {
                result.add(SubAccCurrentTaskExecutable.builder()
                        .orgId(orgId)
                        .accountIds(accountIds)
                        .periodId(periodVO.getId())
                        .shardingIndex(VoucherShardingService.getShardingIndex(orgId, periodVO.getId()))
                        .build());
            }
        }

        return result;
    }

    private List<Long> fetchBaseData(String selectSql, String field) {
        try {
            if (selectSql == null || selectSql.trim().length() == 0) {
                return Collections.emptyList();
            }
            String[] split = selectSql.split(";");
            JdbcTemplate jdbcTemplate = DB.getJdbcTemplate(split[0]);
            List<Long> result = jdbcTemplate.queryForList(split[1], Long.class);
            Assert.isTrue(Integer.parseInt(split[2]) == result.size(),
                    String.format("查询结果与预期数量不符，预期数量%s，实际数量%s", split[2], result.size()));
            return result;
        } catch (Exception e) {
            throw new ParseConfigException(field + "字段解析异常：" + e.getClass().getName() + ":" + e.getMessage(), e);
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    @Slf4j
    @SuperBuilder
    @NoArgsConstructor
    private static class InitReciStateTaskExecutable extends AccCurrentTaskExecutable {

        private List<Long> orgIds;
        private List<Long> accountIds;
        private long endInitPeriodId;

        @Override
        public boolean supportSplit() {
            return false;
        }

        @Override
        public void execute() {
            @Cleanup RowsWriter rowsWriter = new RowsWriter(DB.getCopyHelper(), CopyHelper::copyReciInitStat);
            List<BookService.BookVO> bookVOS = BookService.getBookVOsByOrg(orgIds);
            for (BookService.BookVO bookVO : bookVOS) {
                RowsBuilder rowsBuilder = new RowsBuilder(bookVO);
                for (Long accountId : accountIds) {
                    rowsWriter.write(rowsBuilder.buildReciInitState(ID.genLongId(), accountId, endInitPeriodId));
                }
            }
        }
    }

    @EqualsAndHashCode(callSuper = true)
    @Data
    @Slf4j
    @SuperBuilder
    @NoArgsConstructor
    private static class SubAccCurrentTaskExecutable extends AccCurrentTaskExecutable {

        private List<Long> accountIds;
        private Long orgId;
        private Long periodId;
        private int shardingIndex;
        private static final String SQL_SELECT = "select v.fbizdate,ve.* from t_gl_voucher$%s v " +
                "inner join t_gl_voucherentry$%s ve on v.fid = ve.fid where ve.forgid = %s and ve.fperiodid = %s and  ve.faccountid in(%s)";
        private static final String SQL_UPDATE = "update t_gl_voucherentry$%s set fexpiredate = ? where fentryid = ?";
        @Override
        public boolean supportSplit() {
            return false;
        }


        @Override
        public void execute() {
            @Cleanup RowsWriter rowsWriter = new RowsWriter(DB.getCopyHelper(), CopyHelper::copyAccCurrent);
            BookService.BookVO bookVO = BookService.get(orgId);
            RowsBuilder rowsBuilder = new RowsBuilder(bookVO, periodId, 1L);
            List<Object[]> updateParams = new ArrayList<>(1000);
            DB.getFiJdbcTemplate().query(con -> PrepareStatements.cursor(con, String.format(SQL_SELECT, shardingIndex,
                    shardingIndex, orgId, periodId, accountIds.stream().map(String::valueOf).collect(Collectors.joining(",")))), rs -> {
                while (rs.next()) {
                    String entryDC = rs.getString("fentrydc");
                    rowsWriter.write(rowsBuilder.buildAccCurrent(
                            ID.genLongId(),
                            rs.getLong("faccountid"),
                            rs.getLong("fassgrpid"),
                            entryDC.equals("1") ? rs.getLong("foriginaldebit") : -1 * rs.getLong("foriginalcredit"),
                            entryDC.equals("1") ? rs.getLong("flocaldebit") : -1 * rs.getLong("flocalcredit"),
                            rs.getString("fbizdate"),
                            rs.getLong("fentryid"),
                            rs.getLong("fid"),
                            entryDC));
                    updateParams.add(new Object[]{rs.getDate("fbizdate"), rs.getLong("fentryid")});
                    if (updateParams.size() >= 1000) {
                        DB.getFiJdbcTemplate().batchUpdate(String.format(SQL_UPDATE, shardingIndex), updateParams);
                        updateParams.clear();
                    }
                }
            });
            if (!updateParams.isEmpty()) {
                DB.getFiJdbcTemplate().batchUpdate(String.format(SQL_UPDATE, shardingIndex), updateParams);
            }
        }
    }


    public static void main(String[] args) {
        JdbcTemplateContainer container = new JdbcTemplateContainer();
        DBConfig dbConfig = new DBConfig();
        dbConfig.setHost("172.20.198.10");
        dbConfig.setPort("3306");
        dbConfig.setUser("fitest");
        dbConfig.setPassword("Cosmic@2023");
        dbConfig.setFiDatabase("benchmark_fi");
        dbConfig.setSysDatabase("benchmark_sys");
        container.init(dbConfig);
        container.getFi().query(
                (PreparedStatementCreator) con -> {
                    con.setAutoCommit(false);
                    return con.prepareStatement("select * from t_gl_voucher$11", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                },
                rs -> {
                    while (rs.next()) {
                        System.out.println(rs.getLong("fentryid"));
                    }
                });
    }
}
