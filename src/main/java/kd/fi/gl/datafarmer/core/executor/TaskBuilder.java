package kd.fi.gl.datafarmer.core.executor;

import kd.fi.gl.datafarmer.core.task.impl.FixCashFlowTask;
import kd.fi.gl.datafarmer.core.task.impl.FixEquityBalanceTask;
import kd.fi.gl.datafarmer.core.task.impl.FixSumBalanceTask;
import kd.fi.gl.datafarmer.core.task.impl.FixVoucherCountTask;
import kd.fi.gl.datafarmer.core.task.impl.RebuildBalanceIndexTask;
import kd.fi.gl.datafarmer.core.task.impl.RebuildSumBalanceIndexTask;
import kd.fi.gl.datafarmer.core.task.impl.RebuildVoucherIndexTask;
import kd.fi.gl.datafarmer.core.task.impl.VoucherIrrigateTask;
import kd.fi.gl.datafarmer.core.util.AssGrpBuilder;
import kd.fi.gl.datafarmer.core.util.BookService;
import kd.fi.gl.datafarmer.core.util.OrgBuilder;
import kd.fi.gl.datafarmer.core.util.PeriodVOBuilder;
import kd.fi.gl.datafarmer.dto.TaskConfigGroupDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class TaskBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(TaskBuilder.class);

    public static List<VoucherIrrigateTask> buildVoucherIrrigateTasks(TaskConfigGroupDTO configDTO) {
        try {
            // 获取账簿
            List<BookService.BookVO> bookVOS = configDTO.getBookVOS();
            List<PeriodVOBuilder.PeriodVO> periodVOList = configDTO.getPeriodVOS();
            // 获取全部所需科目
            List<Long> accountIds = configDTO.getAccountIds();
            // 获取全部所需维度
            AssGrpBuilder assGrpBuilder = new AssGrpBuilder(configDTO.getPeriodVOS(), configDTO.getAssgrpIds());
            // 根据组织+期间拆分任务
            return periodVOList.stream()
                    .flatMap(periodVO -> bookVOS.stream().map(bookVO -> Arrays.asList(bookVO, periodVO)))
                    .map(bookPeriodList -> {
                                // 当前期间对应的维度集合
                                List<Long> assGrpIds = assGrpBuilder.getAssGrpIds(((PeriodVOBuilder.PeriodVO) bookPeriodList.get(1)).getId());
                                // 头行比
                                int entryRatio = configDTO.getEntryRatio();
                                // 科目维度组合数
                                int CCIDSize = accountIds.size() * assGrpIds.size();
                                if (CCIDSize % 2 != 0) {
                                    throw new IllegalArgumentException("CCID count is required even number. ");
                                }
                                // 组织维度重复次数
                                int repetition = configDTO.getRepetition();
                                // 创建任务
                                return new VoucherIrrigateTask(
                                        // 组织ID从账簿中获取
                                        (BookService.BookVO) bookPeriodList.get(0),
                                        (PeriodVOBuilder.PeriodVO) bookPeriodList.get(1),
                                        configDTO.getEntryCurrencyId(),
                                        entryRatio,
                                        // 组织期间科目，乱序处理
                                        accountIds,
                                        // 当前期间对应的维度集合
                                        assGrpIds,
                                        // 重复次数
                                        repetition,
                                        configDTO.isContainsCashFlow(),
                                        configDTO.getMainCFItemIds(),
                                        configDTO.getSuppCFItemIds(),
                                        configDTO.getDistinctSign(),
                                        configDTO.getAccountType());
                            }

                    ).collect(Collectors.toList());
        } catch (Throwable e) {
            LOG.error("Tasks build failed: " + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static List<Callable<?>> buildRebuildIndexTasks() {
        Set<Callable<?>> result = new HashSet<>(200);
        result.add(new RebuildSumBalanceIndexTask());
        List<Long> orgIds = OrgBuilder.getAllOrgIds();
        List<PeriodVOBuilder.PeriodVO> periodVOList = new PeriodVOBuilder(2L, "202007", "202306").getPeriodVOList();
        for (Long orgId : orgIds) {
            for (PeriodVOBuilder.PeriodVO periodVO : periodVOList) {
                result.add(new RebuildVoucherIndexTask(orgId, periodVO.getId()));
                result.add(new RebuildBalanceIndexTask(orgId));
            }
        }
        return new ArrayList<>(result);
    }

    public static List<Callable<?>> buildFixSumBalanceTasks() {
        List<Long> orgIds = OrgBuilder.getAllOrgIds();
        return orgIds.stream().map(FixSumBalanceTask::new).collect(Collectors.toList());
    }

    public static List<Callable<?>> buildFixCashFlowTasks() {
        List<Long> orgIds = OrgBuilder.getAllOrgIds();
        return orgIds.stream().map(FixCashFlowTask::new).collect(Collectors.toList());
    }

    public static List<Callable<?>> buildFixVoucherCountTasks() {
        List<Long> orgIds = OrgBuilder.getAllOrgIds();
        return orgIds.stream().map(FixVoucherCountTask::new).collect(Collectors.toList());
    }

    public static List<Callable<?>> buildFixEquityBalanceTask() {
        List<Long> orgIds = OrgBuilder.getAllOrgIds();
        return orgIds.stream().map(FixEquityBalanceTask::new).collect(Collectors.toList());
    }


}
