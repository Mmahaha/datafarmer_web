package kd.fi.gl.datafarmer.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class ProgressRecorder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProgressRecorder.class);

    private final String curTask;

    private final int total;

    private final AtomicInteger progress = new AtomicInteger(0);

    public ProgressRecorder(String curTask, int total) {
        this.curTask = curTask;
        this.total = total;
    }

    public void incrementAndLog(long seconds) {
        LOGGER.info("{} progress:{}/{}, cost {} s", curTask, progress.incrementAndGet(), total, seconds);
    }

}
