package stream;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.CustomNoParalleSource;

import java.util.ArrayList;
import java.util.List;

public class SplitStreamDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> streamSource = environment.addSource(new CustomNoParalleSource());

        SplitStream<Long> splitStream = streamSource.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {
                List<String> stream = new ArrayList<String>();
                if (value % 2 == 0) {
                    stream.add("even");
                } else {
                    stream.add("odd");
                }
                return stream;
            }
        });

        DataStream<Long> evenStream = splitStream.select("even");
        evenStream.print().setParallelism(1);
        environment.execute("SplitStreamDemo");

    }
}
