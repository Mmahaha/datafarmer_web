package kd.fi.gl.datafarmer.core.util;

import org.springframework.jdbc.core.RowCallbackHandler;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PeriodVOBuilder {

    private final List<PeriodVO> periodVOList;

    public PeriodVOBuilder(long periodTypeId, String startPeriodNumber, String endPeriodNumber) {
        periodVOList = new ArrayList<>(36);
        DB.getFiJdbcTemplate().query("select fid,fbegindate,fenddate from t_bd_period" +
                " where ftypeid = ? and fnumber >= ? and fnumber <= ? and fisadjustperiod = ?",
                new Object[]{periodTypeId, startPeriodNumber, endPeriodNumber, "0"},
                new int[] {Types.BIGINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR},
                (RowCallbackHandler)  r -> periodVOList.add(new PeriodVO(r.getLong("fid"), r.getDate("fbegindate"), r.getDate("fenddate"))));
    }

    public List<PeriodVO> getPeriodVOList() {
        return periodVOList;
    }

    public static class PeriodVO {
        private final long id;

        private final Date beginDate;

        private final Date endDate;

        public PeriodVO(long id, Date beginDate, Date endDate) {
            this.id = id;
            this.beginDate = beginDate;
            this.endDate = endDate;
        }

        public long getId() {
            return id;
        }

        public Date getBeginDate() {
            return beginDate;
        }

        public Date getEndDate() {
            return endDate;
        }
    }
}
