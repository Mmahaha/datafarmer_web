package kd.fi.gl.datafarmer.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class AssGrpBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(AssGrpBuilder.class);
    private final Map<Long, List<Long>> periodIdToAssGrpIdsMap;
    private List<Long> assGrpIds;

    public AssGrpBuilder(List<PeriodVOBuilder.PeriodVO> periodVOS, List<Long> assGrpIds) {
        this.assGrpIds = assGrpIds;
        List<Long> periodIds = periodVOS.stream().map(PeriodVOBuilder.PeriodVO::getId).collect(Collectors.toList());
        // 按期间均分
        int periodIdSize = periodIds.size();
        // 每批次大小
        int batchSize = assGrpIds.size() / periodIdSize;
//        if (batchSize == 0) {
//            // 维度太少
//            throw new IllegalArgumentException(
//                    String.format(
//                            "The number of assGrpIds is not sufficient to allocate for all periods, %s, periodCnt: %s",
//                            this.assGrpIds.size(), periodIdSize
//                    )
//            );
//        }
        // 将核算维度拆分
        List<List<Long>> assGrpIdsPartitions = IntStream.range(0, periodIdSize)
                .mapToObj(i -> assGrpIds.subList(i * batchSize, i == periodIdSize - 1 ? assGrpIds.size() : (i + 1) * batchSize))
                .collect(Collectors.toList());
        // 分配给每个期间
        this.periodIdToAssGrpIdsMap = IntStream.range(0, periodIdSize).boxed()
                .collect(Collectors.toMap(periodIds::get, assGrpIdsPartitions::get));
//        LOG.info("AssGrpBuildResult,config: {}, totalSize: {}, periodCnt: {}, batchSize: {}", configVO, this.assGrpIds.size(), periodIdSize, batchSize);
    }

    public List<Long> getAssGrpIds(long periodId) {
        List<Long> results = periodIdToAssGrpIdsMap.get(periodId);
//        LOG.info("AssGrpBuildResult,bookId: {},periodId: {},size: {}", bookId, periodId, results.size());
        return results;
    }

}
