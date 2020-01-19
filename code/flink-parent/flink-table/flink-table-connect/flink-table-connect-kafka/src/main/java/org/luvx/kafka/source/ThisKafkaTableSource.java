package org.luvx.kafka.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.sources.StreamTableSource;

/**
 * @ClassName: org.luvx.kafka.source
 * @Description:
 * @Author: Ren, Xie
 */
public class KafkaTableSource implements StreamTableSource<String> {
    @Override
    public DataStream<String> getDataStream(StreamExecutionEnvironment execEnv) {
        return null;
    }

    @Override
    public TableSchema getTableSchema() {
        return null;
    }
}
