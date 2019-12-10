package org.luvx.entity;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

/**
 * @ClassName: org.luvx.common.entity
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/12/10 18:22
 */
@AllArgsConstructor
@NoArgsConstructor
        public class WordWithCount {
    public String word;
    public long   cnt;

    @Override
    public String toString() {
        return "WC " + word + " " + cnt;
    }
}