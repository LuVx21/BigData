package org.luvx.function;

import lombok.Data;

/**
 * @ClassName: org.luvx.entity
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/27 14:52
 */
@Data
public class ItemViewCount {
    /**
     * 商品 ID
     */
    private long itemId;

    /**
     * 窗口结束时间戳
     */
    private long windowEnd;

    /**
     * 商品点击数
     */
    private long viewCount;

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.setItemId(itemId);
        result.setWindowEnd(windowEnd);
        result.setViewCount(viewCount);
        return result;
    }
}
