package org.luvx.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.Properties;

/**
 * @ClassName: org.luvx.common.utils
 * @Description:
 * @Author: Ren, Xie
 * @Date: 2019/12/9 16:17
 */
@Slf4j
public class KafkaConfigUtils {
    public static Properties getProducerProp() {
        return PropertiesUtils.getProperties("config/kafka/kafka-producer.properties");
    }

    public static Properties getConsumerProp() {
        return PropertiesUtils.getProperties("config/kafka/kafka-consumer.properties");
    }
}