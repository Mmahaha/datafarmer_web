package kd.fi.gl.datafarmer.core.util.helper;

import lombok.SneakyThrows;
import org.postgresql.copy.CopyManager;
import org.postgresql.core.BaseConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class CopyHelper implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CopyHelper.class);
    private static final String COPY_HEAD_SQL = "copy t_gl_voucher$%d (fid,fnumber,fbizdate,fbookeddate,fcreatetime,fauditdate,fposttime,fmodifytime,fcashierid,fmainstatus,fsuppstatus,fischeck,fbooktypeid,fbookid,forgid,fperiodid,ftypeid,fbillstatus,fcreatorid,fsubmitterid,fauditorid,fispost,fposterid,fmodifierid,floccurrency)" +
            " from stdin with csv";
    private static final String COPY_ENTRY_SQL = "copy t_gl_voucherentry$%d (fentryid, fid, faccountid, fcurrencyid, flocalexchangerate, fassgrpid, foriginaldebit, foriginalcredit, flocaldebit, flocalcredit, fseq, fentrydc, forgid, fperiodid, fmaincfamount,fsuppcfamount,fmaincfitemid,fsuppcfitemid)" +
            " from stdin with csv";
    private static final String COPY_$PK_SQL = "copy t_gl_voucher$pk (fpk, findex, fnumber)" +
            " from stdin with csv";
    private static final String COPY_BAL_$PK_SQL = "copy t_gl_balance$pk (fpk, findex)" +
            " from stdin with csv";

    private static final String COPY_BALANCE_SQL = "copy t_gl_balance (fid, fperiodid, fendperiodid, faccounttableid, faccountid, fcurrencyid, fbeginfor, fbeginlocal, fdebitfor, fdebitlocal, fcreditfor, fcreditlocal, fyeardebitfor, fyeardebitlocal, fyearcreditfor, fyearcreditlocal, fendfor, fendlocal, fcount, forgid, fbooktypeid, fassgrpid)" +
            " from stdin with csv";

    private static final String COPY_CASH_FLOW_SQL = "copy t_gl_cashflow (fid,fperiodid,fcfitemid,forgid,famount,fbooktypeid,fyearamount,fendperiodid,fcount,fcurrencyid)" +
            " from stdin with csv";

    private static final String COPY_SUM_BALANCE_SQL = "copy t_gl_balance_accsum (fid, fperiodid, fendperiodid, faccounttableid, faccountid, fcurrencyid, fbeginfor, fbeginlocal, fdebitfor, fdebitlocal, fcreditfor, fcreditlocal, fyeardebitfor, fyeardebitlocal, fyearcreditfor, fyearcreditlocal, fendfor, fendlocal, fcount, forgid, fbooktypeid)" +
            " from stdin with csv";

    private static final String COPY_COUNT_SQL = "copy t_gl_vouchercount (fid,forgid,fbooktypeid,fperiodid,fbillstatus,fsourcetype,fvouchercount,fentrycount,fischeck,fispost,fbookeddate)" +
            " from stdin with csv";



    private final CopyManager copyManager;

    private Connection connection;

    CopyHelper() {
        this.copyManager = null;
    }

    public CopyHelper(Connection connection) {
        try {
            this.connection = connection;
            this.copyManager = new CopyManager(connection.unwrap(BaseConnection.class));
        } catch (SQLException e) {
            logger.error("CopyHelper init error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 将csv格式的凭证头值集写入数据库
     *
     * @param csvStr       形如 ["123,456","234,456"] 的csv字符串列表
     */
    public long copyVoucherHead(int shardingIndex, List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            return copyManager.copyIn(String.format(COPY_HEAD_SQL, shardingIndex), new StringReader(String.join("\n", csvStr)));
        } catch (Exception e) {
            logger.error("copy voucher head error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 将csv格式的凭证分录值集写入数据库
     *
     * @param shardingIndex 凭证表分片索引
     * @param csvStr       形如 ["123,456","234,456"] 的csv字符串列表
     */
    public long copyVoucherEntry(int shardingIndex, List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            return copyManager.copyIn(String.format(COPY_ENTRY_SQL, shardingIndex), new StringReader(String.join("\n", csvStr)));
        } catch (Exception e) {
            logger.error("copy voucher entry error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 将csv格式的凭证快速索引值集写入数据库
     *
     * @param csvStr       形如 ["123,456","234,456"] 的csv字符串列表
     */
    public long copyVoucher$PK(List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            return copyManager.copyIn(COPY_$PK_SQL, new StringReader(String.join("\n", csvStr)));
        } catch (Exception e) {
            logger.error("copy voucher$pk error", e);
            throw new RuntimeException(e);
        }
    }

    public long copyBalance$PK(List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            long l = copyManager.copyIn(COPY_BAL_$PK_SQL, new StringReader(String.join("\n", csvStr)));
//            connection.commit();
            return l;

        } catch (Exception e) {
            logger.error("copy voucher$pk error", e);
            throw new RuntimeException(e);
        }
    }


    /**
     * 将csv格式的余额值集写入数据库
     *
     * @param csvStr       形如 ["123,456","234,456"] 的csv字符串列表
     */
    public long copyBalance(List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            return copyManager.copyIn(COPY_BALANCE_SQL, new StringReader(String.join("\n", csvStr)));
        } catch (Exception e) {
            logger.error("copy balance error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 将csv格式的现金流量余额值集写入数据库
     *
     * @param csvStr       形如 ["123,456","234,456"] 的csv字符串列表
     */
    public long copyCashFlow(List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            return copyManager.copyIn(COPY_CASH_FLOW_SQL, new StringReader(String.join("\n", csvStr)));
        } catch (Exception e) {
            logger.error("copy cashflow error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 将csv格式的汇总余额值集写入数据库
     *
     * @param csvStr       形如 ["123,456","234,456"] 的csv字符串列表
     */
    public long copySumBalance(List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            return copyManager.copyIn(COPY_SUM_BALANCE_SQL, new StringReader(String.join("\n", csvStr)));
        } catch (Exception e) {
            logger.error("copy sumbalance error", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 将csv格式的凭证计数值集写入数据库
     *
     * @param csvStr       形如 ["123,456","234,456"] 的csv字符串列表
     */
    public long copyVoucherCount(List<String> csvStr) {
        if (csvStr.isEmpty()) {return 0L;}
        try {
            return copyManager.copyIn(COPY_COUNT_SQL, new StringReader(String.join("\n", csvStr)));
        } catch (Exception e) {
            logger.error("copy vouchercount error", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    @SneakyThrows
    public void close(){
        if (this.connection != null) {
            this.connection.close();
            this.connection = null;
        }
    }
}
