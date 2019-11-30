package org.luvx.utils;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.luvx.entity.UserBehavior;

import java.util.Properties;
import java.util.Random;

/**
 * 造数据用(send to kafka)
 */
public class KafkaUtils2 {

    public static final String topic = "flink";

    public static void main(String[] args) throws InterruptedException {
        for (; ; ) {
            Thread.sleep(5 * 1000);
            send();
        }
    }

    private static void send() {
        Properties props = KafkaUtils.getProducerProp();
        Producer<String, String> producer = new KafkaProducer<>(props);

        UserBehavior user = make();
        String msg = JSON.toJSONString(user);
        producer.send(new ProducerRecord<>(topic, null, null, msg));

        System.out.println("发送数据: " + msg);
        producer.flush();
    }

    private static UserBehavior make() {
        String[] a = {"pv", "buy", "cart", "fav"};
        // [0, 3]
        Random r = new Random();
        int i = r.nextInt(4) % (4);

        return UserBehavior.builder()
                .userId(System.currentTimeMillis() / 1000)
                .itemId(2001L)
                .categoryId(101)
                .behavior(a[i])
                .timestamp(System.currentTimeMillis())
                .build();
    }
}
