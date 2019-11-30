package org.luvx.entity;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * @ClassName: org.luvx.entity
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/27 14:54
 */
public class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(UserBehavior userBehavior, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}