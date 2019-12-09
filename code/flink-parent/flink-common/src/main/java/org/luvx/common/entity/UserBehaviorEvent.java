package org.luvx.common.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @ClassName: org.luvx.entity
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/11/27 14:44
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class UserBehaviorEvent {
    /**
     * 用户id
     */
    private Long userId;

    /**
     * 商品id
     */
    private Long itemId;

    /**
     * 商品种类id
     */
    private Integer categoryId;

    /**
     * 用户行为
     * pv, buy, cart, fav
     */
    private String behavior;

    /**
     * 时间戳
     */
    private Long timestamp;
}
