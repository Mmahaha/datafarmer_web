package kd.fi.gl.datafarmer.core.util;

import kd.fi.gl.datafarmer.core.config.BizConfigVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * 科目数据构造
 */
public class AccountBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AccountBuilder.class);

    private List<Long> accountIds;

    public AccountBuilder(BizConfigVO configVO, BookService.BookVO bookVO) {
        int accountLimit = FastStringUtils.extractLimitFromLimitOffsetStr(configVO.getAccountLimit());
        accountIds = new ArrayList<>(accountLimit);
        List<String> accountTypeIds = AccountTypeService.getAccountTypeIds(bookVO.getAccountTableId(), configVO.getAccountType())
                .stream().map(String::valueOf).collect(Collectors.toList());
        if (accountTypeIds.isEmpty()) {
            throw new IllegalArgumentException("无法过滤出任何科目类型，config=" + configVO);
        }
        // 正常的非现金科目
        DB.getFiJdbcTemplate().query(String.format("select fid from t_bd_account where faccounttypeid in (%s)" +
                "and fisleaf = '1' and fenable = '1' and faccounttableid = %s order by fid %s",
                        String.join(",", accountTypeIds), bookVO.getAccountTableId(), configVO.getAccountLimit()),
                (RowCallbackHandler) rs -> accountIds.add(rs.getLong(1))
        );
        if (accountIds.size() != accountLimit) {
            throw new IllegalArgumentException(String.format("科目数量不足，需%s，实际%s，config=%s", accountLimit, accountIds.size(), configVO));
        }
        // 现金
        if (configVO.isContainsCash()) {
            int cashAccountLimit = FastStringUtils.extractLimitFromLimitOffsetStr(configVO.getCashAccountLimit());
            List<Long> cashAccountIds = new ArrayList<>(cashAccountLimit);
            DB.getFiJdbcTemplate().query("select fid from t_bd_account where (fiscash = '1' or fisbank = '1' or fiscashequivalent = '1') " +
                    "and fisleaf = '1' and fenable = '1' and faccounttableid = " + bookVO.getAccountTableId() +" order by fid " + configVO.getCashAccountLimit(), rs -> {cashAccountIds.add(rs.getLong(1));}
            );
            if (cashAccountLimit != cashAccountIds.size()) {
                throw new IllegalArgumentException(String.format("现金科目数量不足，需%s，实际%s，config=%s", cashAccountLimit, cashAccountIds.size(), configVO));
            }
            if (cashAccountLimit != accountLimit) {
                throw new IllegalArgumentException("现金科目与非现非损科目数量不一致, config=" + configVO);
            }
            // 将现金科目交错插入accountIds
            accountIds = LongStream.range(0, cashAccountLimit)
                    .flatMap(i -> LongStream.of(cashAccountIds.get((int) i), accountIds.get((int) i)))
                    .boxed().collect(Collectors.toList());
        }
    }

    public List<Long> getAccountIds() {
        return accountIds;
    }
}
