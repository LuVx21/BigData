package org.luvx.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.luvx.common.utils.DateTimeUtils;

/**
 * @ClassName: org.luvx.common.entity
 * @Description:
 * @Author: Ren, Xie
 */
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
public class LogEvent {
    private String ip;
    private Long   time;
    private String timeZone;
    private String type;
    private String url;

    public static LogEvent of(String[] tokens) {
        return LogEvent.builder()
                .ip(tokens[0]).time(DateTimeUtils.str2EpochMilli(tokens[3])).timeZone(tokens[4])
                .type(tokens[5]).url(tokens[6])
                .build();
    }
}
