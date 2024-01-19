package kd.fi.gl.datafarmer.core.util;

import java.util.HashMap;
import java.util.Map;

/**
 * 凭证计数累加器，按记账日期隔离
 */
public class VoucherCountAccumulator {

    private final Map<String, VoucherCount> voucherCountMap = new HashMap<>(32, 1.0f);

    public void accumulate(String bookedDate, long headCount, long entryCount, long pkId) {
        voucherCountMap.compute(bookedDate, (date, count) -> {
            if (count == null) {return new VoucherCount(headCount, entryCount, pkId);}
            count.headCount += headCount;
            count.entryCount += entryCount;
            return count;
        });
    }

    public Map<String, VoucherCount> getVoucherCountMap() {
        return voucherCountMap;
    }

    public static class VoucherCount {
        private final long pkId;
        private long headCount;
        private long entryCount;

        private VoucherCount(long headCount, long entryCount, long pkId) {
            this.pkId = pkId;
            this.headCount = headCount;
            this.entryCount = entryCount;
        }

        public long getHeadCount() {
            return headCount;
        }

        public long getEntryCount() {
            return entryCount;
        }

        public long getPkId() {
            return pkId;
        }
    }
}
