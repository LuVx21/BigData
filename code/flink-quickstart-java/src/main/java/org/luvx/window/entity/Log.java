package org.luvx.window.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.luvx.utils.TimeUtils;

/**
 * @ClassName: org.luvx.window.entity
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/12/6 9:34
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class Log {
    private String ip;
    private Long   time;
    private String timeZone;
    private String type;
    private String url;

    public static Log of(String[] tokens) {
        return Log.builder()
                .ip(tokens[0]).time(TimeUtils.str2EpochMilli(tokens[3])).timeZone(tokens[4])
                .type(tokens[5]).url(tokens[6])
                .build();
    }
}
