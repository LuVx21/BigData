package org.luvx.datasource;

import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class DataSourceFromFile extends RichSourceFunction<String> {
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (true) {
            sourceContext.collect("");
        }
    }

    @Override
    public void cancel() {
    }
}
