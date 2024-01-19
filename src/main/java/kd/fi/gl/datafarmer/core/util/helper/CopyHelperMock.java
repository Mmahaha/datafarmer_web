package kd.fi.gl.datafarmer.core.util.helper;

import java.sql.Connection;
import java.util.List;

public class CopyHelperMock extends CopyHelper {

    public CopyHelperMock(Connection connection) {

    }

    @Override
    public long copyVoucherHead(int shardingIndex, List<String> csvStr) {
        return csvStr.size();
    }

    @Override
    public long copyVoucherEntry(int shardingIndex, List<String> csvStr) {
        return csvStr.size();
    }

    @Override
    public long copyVoucher$PK(List<String> csvStr) {
        return csvStr.size();
    }

    @Override
    public long copyBalance$PK(List<String> csvStr) {
        return csvStr.size();
    }

    @Override
    public long copyBalance(int shardingIndex, List<String> csvStr) {
        return csvStr.size();
    }

    @Override
    public long copyCashFlow(List<String> csvStr) {
        return csvStr.size();
    }

    @Override
    public long copySumBalance(List<String> csvStr) {
        return csvStr.size();
    }

    @Override
    public long copyVoucherCount(List<String> csvStr) {
        return csvStr.size();
    }
}
