package source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class CustomParalleSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> longDataStreamSource = executionEnvironment.addSource(new CustomParalleSource());
        DataStream<Long> dataStream = longDataStreamSource.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                System.out.println("received data" + aLong);
                return aLong;
            }
        });

        DataStream<Long> sum = dataStream.timeWindowAll(Time.seconds(2)).sum(0);
        executionEnvironment.setParallelism(1);
        sum.print();
        executionEnvironment.execute(CustomParalleSourceDemo.class.getSimpleName());
    }
}
