package kd.fi.gl.datafarmer.core.util;

import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * 账簿服务，一次性初始化全部账簿信息缓存
 */
public class BookService {

    private static final Map<Long, BookVO> BOOK_VO_MAP;

    static {
        BOOK_VO_MAP = new HashMap<>(50000);
        AtomicInteger index = new AtomicInteger(1);
        DB.getFiJdbcTemplate().query("select fid,forgid,faccounttableid,fperiodtypeid,fbasecurrencyid," +
                        "fbookstypeid from t_bd_accountbooks where fstartperiodid > 0 order by fcreatetime asc",
                (RowCallbackHandler) rs -> BOOK_VO_MAP.put(rs.getLong("forgid"),
                        new BookVO(rs.getLong("forgid"),
                                rs.getLong("faccounttableid"),
                                rs.getLong("fperiodtypeid"),
                                rs.getLong("fid"),
                                index.getAndIncrement(),
                                rs.getLong("fbasecurrencyid"),
                                rs.getLong("fbookstypeid")))
        );
    }

    /**
     * 根据组织ID获取账簿
     *
     * @param orgId 组织id
     * @return 账簿
     */
    public static BookVO get(long orgId) {
        BookVO bookVO = BOOK_VO_MAP.get(orgId);
        if (bookVO == null) {
            throw new RuntimeException("Book id not exists: " + orgId);
        }
        return bookVO;
    }

    public static List<BookVO> getBookVOsByOrg(List<Long> orgIds) {
        return orgIds.stream().map(BookService::get).collect(Collectors.toList());
    }


    public static class BookVO {
        private final long orgId;

        private final long accountTableId;

        private final long periodTypeId;

        private final long id;

        private final int index;

        private final long localCurrencyId;

        private final long bookTypeId;


        public BookVO(long orgId, long accountTableId, long periodTypeId, long id, int index, long localCurrencyId, long bookTypeId) {
            this.orgId = orgId;
            this.accountTableId = accountTableId;
            this.periodTypeId = periodTypeId;
            this.id = id;
            this.index = index;
            this.localCurrencyId = localCurrencyId;
            this.bookTypeId = bookTypeId;
        }

        public long getOrgId() {
            return orgId;
        }

        public long getAccountTableId() {
            return accountTableId;
        }

        public long getPeriodTypeId() {
            return periodTypeId;
        }

        public long getId() {
            return id;
        }

        public int getIndex() {
            return index;
        }

        public long getLocalCurrencyId() {
            return localCurrencyId;
        }

        public long getBookTypeId() {
            return bookTypeId;
        }

        @Override
        public String toString() {
            return "BookVO{" +
                    "orgId=" + orgId +
                    ", accountTableId=" + accountTableId +
                    ", periodTypeId=" + periodTypeId +
                    ", id=" + id +
                    ", index=" + index +
                    ", localCurrencyId=" + localCurrencyId +
                    ", bookTypeId=" + bookTypeId +
                    '}';
        }
    }

}
