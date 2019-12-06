package org.luvx.window;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.luvx.window.entity.Log;

import javax.annotation.Nullable;

/**
 * @ClassName: org.luvx.window
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/12/6 10:29
 */
public class Watermarks1 implements AssignerWithPeriodicWatermarks<Log> {
    private       Long currentMaxTimestamp = 0L;
    private final Long maxOutOfOrderness   = 10 * 1000L;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Log element, long previousElementTimestamp) {
        long time = element.getTime();
        currentMaxTimestamp = Math.max(time, previousElementTimestamp);
        /// getCurrentWatermark().getTimestamp();
        return time;
    }
}
