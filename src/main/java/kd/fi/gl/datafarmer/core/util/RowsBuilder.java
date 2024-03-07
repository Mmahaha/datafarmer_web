package kd.fi.gl.datafarmer.core.util;

import kd.fi.gl.datafarmer.core.bean.AcctSumAmtInfo;
import kd.fi.gl.datafarmer.core.bean.AmtInfo;
import kd.fi.gl.datafarmer.core.bean.CashInfo;

import java.util.StringJoiner;

public class RowsBuilder {

    private final String orgIdStr;
    private final String bookTypeIdStr;
    private final String periodIdStr;
    private final String bookIdStr;
    private final String accountableIdStr;
    private final String localCurrencyIdStr;
    private final String entryCurrencyIdStr;
    private final boolean tempStatus;
    /**
     * 凭证数据行后缀，因为一行头数据除了id和凭证号其他都是一样的，所以其他的值只需在初始化时构造一次
     */
    private String voucherHeadValueSuffix;


    public RowsBuilder(BookService.BookVO bookVO, long periodId, long entryCurrencyId, boolean tempStatus) {
        this.orgIdStr = String.valueOf(bookVO.getOrgId());
        this.bookTypeIdStr = String.valueOf(bookVO.getBookTypeId());
        this.bookIdStr = String.valueOf(bookVO.getId());
        this.localCurrencyIdStr = String.valueOf(bookVO.getLocalCurrencyId());
        this.entryCurrencyIdStr = String.valueOf(entryCurrencyId);
        this.periodIdStr = String.valueOf(periodId);
        this.accountableIdStr = String.valueOf(bookVO.getAccountTableId());
        initVoucherHeadValueSuffix();
        this.tempStatus = tempStatus;
    }

    public RowsBuilder(BookService.BookVO bookVO, long periodId, long entryCurrencyId) {
        this(bookVO, periodId, entryCurrencyId, false);
    }

    public RowsBuilder(BookService.BookVO bookVO) {
        this(bookVO, 0L, 0L, false);
    }


    // 构造单张凭证头的数据行
    public String buildVoucherHead(int voucherIndex, long voucherId, String billno, String bookedDate, boolean isCash) {
        StringJoiner resultAppender = new StringJoiner(",");
        resultAppender.add(String.valueOf(voucherId))//fid
                .add(billno)//fnumber
                .add(bookedDate)//fbizdate
                .add(bookedDate)//fbookeddate
                .add(bookedDate)//fcreatetime
                .add(bookedDate)//fauditdate
                .add(bookedDate)//fposttime
                .add(bookedDate);//fmodifytime
        if (isCash) {
            resultAppender.add("1,3,3,c");//fcashierid,fmainstatus,fsuppstatus,fischeck
        } else {
            resultAppender.add("0,0,0,a");//fcashierid,fmainstatus,fsuppstatus,fischeck
        }
        resultAppender.add(voucherHeadValueSuffix);
        return resultAppender.toString();
    }



    // 构造单张凭证分录的数据行
    public String buildVoucherEntry(long entryId, long voucherId, int entrySeq, long accountId, long assgrpId, boolean debitDC,
                                    AmtInfo amtInfo, CashInfo cashInfo) {
        StringJoiner stringJoiner = new StringJoiner(",");
        stringJoiner.add(String.valueOf(entryId))//fentryid
                .add(String.valueOf(voucherId))//fid
                .add(String.valueOf(accountId))//faccountid
                .add(entryCurrencyIdStr)//fcurrencyid
                .add(String.valueOf(amtInfo.getRate()))//flocalexchangerate
                .add(String.valueOf(assgrpId))//fassgrpid
                .add(String.valueOf(debitDC ? amtInfo.getOriAmt() : 0))//foriginaldebit
                .add(String.valueOf(debitDC ? 0 : amtInfo.getOriAmt()))//foriginalcredit
                .add(String.valueOf(debitDC ? amtInfo.getLocAmt() : 0))//flocaldebit
                .add(String.valueOf(debitDC ? 0 : amtInfo.getLocAmt()))//flocalcredit
                .add(String.valueOf(entrySeq))//fseq
                .add(debitDC ? "1" : "-1")//fentrydc
                .add(orgIdStr)//forgid
                .add(periodIdStr)
                .add(String.valueOf(cashInfo.getCfAmount()))//fmaincfamount
                .add(String.valueOf(cashInfo.getCfAmount()))//fsuppcfamount
                .add(String.valueOf(cashInfo.getMainCfItemId()))//fmaincfitemid
                .add(String.valueOf(cashInfo.getSuppCfItemId()));//fsuppcfitemid
        return stringJoiner.toString();
    }

    // 构造单张凭证头快速索引的数据行
    public String buildVoucher$PK(long voucherId, int shardingIndex, String billno) {
        return String.join(",", String.valueOf(voucherId), String.valueOf(shardingIndex), billno);
    }

    // 构造余额的数据行
    public String buildBalance(long fid, long accountId, long assgrpId,  boolean debitDC, AmtInfo amtInfo, int repetition) {
        long amountFor = (long) amtInfo.getOriAmt() * repetition;
        long amountLoc = (long) amtInfo.getLocAmt() * repetition;
        String debitFor = debitDC ? String.valueOf(amountFor) : "0";
        String debitLoc = debitDC ? String.valueOf(amountLoc) : "0";
        String creditFor = debitDC ? "0" : String.valueOf(amountFor);
        String creditLoc = debitDC ? "0" : String.valueOf(amountLoc);
        StringJoiner stringJoiner = new StringJoiner(",");
        stringJoiner.add(String.valueOf(fid))//fid
                .add(periodIdStr)//fperiodid
                .add("99999999999")//fendperiodid
                .add(accountableIdStr)//faccounttableid
                .add(String.valueOf(accountId))//faccountid
                .add(entryCurrencyIdStr)//fcurrencyid
                .add("0")//fbeginfor
                .add("0")//fbeginlocal
                .add(debitFor)//fdebitfor
                .add(debitLoc)//fdebitlocal
                .add(creditFor)//fcreditfor
                .add(creditLoc)//fcreditlocal
                .add(debitFor)//fyeardebitfor
                .add(debitLoc)//fyeardebitlocal
                .add(creditFor)//fyearcreditfor
                .add(creditLoc)//fyearcreditlocal
                .add(debitDC ? debitFor : "-" + creditFor)//fendfor
                .add(debitDC ? debitLoc : "-" + creditLoc)//fendlocal
                .add(String.valueOf(repetition))//fcount
                .add(orgIdStr)//forgid
                .add(bookTypeIdStr)//fbooktypeid
                .add(String.valueOf(assgrpId));//fassgrpid
        return stringJoiner.toString();
    }

    public String buildCashFlow(long fid, long cfItemId, long assgrpId, long amount, int repetition) {
        long cashAmount = amount * repetition;
        StringJoiner stringJoiner = new StringJoiner(",");
        stringJoiner.add(String.valueOf(fid))//fid
                .add(periodIdStr)//fperiodid
                .add(String.valueOf(cfItemId))//fcfitemid
                .add(String.valueOf(assgrpId))//fassgrpid
                .add(orgIdStr)//forgid
                .add(String.valueOf(cashAmount))//famount
                .add(bookTypeIdStr)//fbooktypeid
                .add(String.valueOf(cashAmount))//fyearamount
                .add("99999999999")//fendperiodid
                .add(String.valueOf(repetition))//fcount
                .add(localCurrencyIdStr);//fcurrencyid
        return stringJoiner.toString();
    }

    public String buildVoucherCount(String bookedDate, VoucherCountAccumulator.VoucherCount voucherCount) {
        StringJoiner stringJoiner = new StringJoiner(",");
        stringJoiner.add(String.valueOf(voucherCount.getPkId()))//fid
                .add(orgIdStr)//forgid
                .add(bookTypeIdStr)//fbooktypeid
                .add(periodIdStr)//fperiodid
                .add(tempStatus ? "A":"C")//fbillstatus
                .add("0")//fsourcetype
                .add(String.valueOf(voucherCount.getHeadCount()))//fvouchercount
                .add(String.valueOf(voucherCount.getEntryCount()))//fentrycount
                .add("a")//fischeck
                .add("1")//fispost
                .add(bookedDate);//fbookeddate
        return stringJoiner.toString();
    }

    public String buildReciInitState(long fid, long accountId, long endInitPeriodId) {
        StringJoiner stringJoiner = new StringJoiner(",");
        stringJoiner.add(String.valueOf(fid))//fid
                .add(orgIdStr)//forgid
                .add(bookTypeIdStr)//fbooktypeid
                .add(accountableIdStr)//faccounttableid
                .add(String.valueOf(accountId))//faccountid
                .add(String.valueOf(endInitPeriodId))//fendinitperiod
                .add("1")//fisendinit
                .add("1")//fmodifier
                .add("2018-08-08");//fmodifydate
        return stringJoiner.toString();
    }

    public String buildAccCurrent(long fid, long accountId, long assgrpId, long amountFor, long amountLoc,
                                  String bizDate, long entryId, long voucherId, String entryDC) {
        StringJoiner stringJoiner = new StringJoiner(",");
        stringJoiner.add(String.valueOf(fid))// fid
                .add(orgIdStr)// forgid
                .add(bookTypeIdStr)// fbooktypeid
                .add(periodIdStr)// fperiodid
                .add(accountableIdStr)// facctableid
                .add(String.valueOf(accountId))// faccountid
                .add(String.valueOf(assgrpId))// fassgrpid
                .add(entryCurrencyIdStr)// fcurrencyid
                .add(String.valueOf(amountFor))// famountfor
                .add(String.valueOf(amountFor))// famountbalfor
                .add(localCurrencyIdStr)// flocalcurrencyid
                .add(String.valueOf(amountLoc))// famount
                .add(String.valueOf(amountLoc))// famountbal
                .add(bizDate)// fbizdate
                .add(bizDate)// fexpiredate
                .add("' '")// fdescription
                .add("0")// fstatus
                .add(String.valueOf(entryId))// fvchentryid
                .add("0")// fsourcetype
                .add(String.valueOf(voucherId))// fvoucherid
                .add("2018-08-08")// fmodifytime
                .add("2018-08-08")// fcreatetime
                .add(String.valueOf(fid))// fmasterid
                .add("1900-01-01")// feffectivedate
                .add("2999-12-31")// funeffectivedate
                .add("1")// fcreatorid
                .add("1")// fwriteoffpersonid
                .add(entryDC)// fentrydc
                .add(bizDate);// fbookeddate
        return stringJoiner.toString();
    }

    public String getOrgIdStr() {
        return orgIdStr;
    }

    public String getPeriodIdStr() {
        return periodIdStr;
    }

    public String getEntryCurrencyIdStr() {
        return entryCurrencyIdStr;
    }

    // 构造汇总余额的数据行
    public String buildSumBalance(long fid, AcctSumAmtInfo sumAmtInfo, int repetition, int assgrpCount) {
        long debitFor = sumAmtInfo.getDebitFor() * repetition;
        long debitLoc = sumAmtInfo.getDebitLoc() * repetition;
        long creditFor = sumAmtInfo.getCreditFor() * repetition;
        long creditLoc = sumAmtInfo.getCreditLoc() * repetition;
        StringJoiner stringJoiner = new StringJoiner(",");
        stringJoiner.add(String.valueOf(fid))//fid
                .add(periodIdStr)//fperiodid
                .add("99999999999")//fendperiodid
                .add(accountableIdStr)//faccounttableid
                .add(String.valueOf(sumAmtInfo.getAccountId()))//faccountid
                .add(entryCurrencyIdStr)//fcurrencyid
                .add("0")//fbeginfor
                .add("0")//fbeginlocal
                .add(String.valueOf(debitFor))//fdebitfor
                .add(String.valueOf(debitLoc))//fdebitlocal
                .add(String.valueOf(creditFor))//fcreditfor
                .add(String.valueOf(creditLoc))//fcreditlocal
                .add(String.valueOf(debitFor))//fyeardebitfor
                .add(String.valueOf(debitLoc))//fyeardebitlocal
                .add(String.valueOf(creditFor))//fyearcreditfor
                .add(String.valueOf(creditLoc))//fyearcreditlocal
                .add(String.valueOf(debitFor - creditFor))//fendfor
                .add(String.valueOf(debitLoc - creditLoc))//fendlocal
                .add(String.valueOf(repetition * assgrpCount))//fcount
                .add(orgIdStr)//forgid
                .add(bookTypeIdStr);//fbooktypeid
        return stringJoiner.toString();
    }


    // 初始化凭证值后缀
    private void initVoucherHeadValueSuffix() {
        StringJoiner resultAppender = new StringJoiner(",");
        resultAppender.add(bookTypeIdStr)//fbooktypeid
                .add(bookIdStr)//fbookid
                .add(orgIdStr)//forgid
                .add(String.valueOf(periodIdStr))//fperiodid
                .add("1323036438454863872")//ftypeid
                .add(tempStatus ? "A" : "C")//fbillstatus
                .add("1")//fcreatorid
                .add("1")//fsubmitterid
                .add("1")//fauditorid
                .add(tempStatus ? "0" : "1")//fispost
                .add("1")//fposterid
                .add("1")//fmodifierid
                .add(localCurrencyIdStr);//floccurrency
        this.voucherHeadValueSuffix = resultAppender.toString();
    }


    public static void main(String[] args) {

    }

}
