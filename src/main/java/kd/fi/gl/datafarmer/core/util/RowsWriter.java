package kd.fi.gl.datafarmer.core.util;

import kd.fi.gl.datafarmer.core.util.helper.CopyHelper;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

import java.io.Flushable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * Description:
 *
 * @author ysj
 * @date 2024/1/31
 */
@RequiredArgsConstructor
public class RowsWriter implements AutoCloseable, Flushable {

    private final CopyHelper copyHelper;
    private List<String> csvStrList = new ArrayList<>(10000);
    private final BiFunction<CopyHelper, List<String>, Long> copyFunc;

    public void write(String csvStr) {
        csvStrList.add(csvStr);
        checkSize();
    }

    private void checkSize() {
        if (csvStrList.size() >= 10000) {
            flush();
        }
    }

    @Override
    public void close() {
        flush();
        if (copyHelper != null) {
            copyHelper.close();
        }
    }

    @Override
    @SneakyThrows
    public void flush() {
        copyFunc.apply(copyHelper, csvStrList);
        csvStrList.clear();
    }
}
