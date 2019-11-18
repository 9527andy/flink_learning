package partition;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.CustomNoParalleSource;

public class CustomPartitionDemo {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Long> dataStreamSource = executionEnvironment.addSource(new CustomNoParalleSource());

        DataStream<Tuple1<Long>> tuple1DataStream = dataStreamSource.map(new MapFunction<Long, Tuple1<Long>>() {
            @Override
            public Tuple1 map(Long aLong) throws Exception {
                return new Tuple1<>(aLong);
            }
        });


        DataStream<Tuple1<Long>> customPartitionStream = tuple1DataStream.partitionCustom(new CustomPartition(), 0);

        DataStream<Object> result = customPartitionStream.map(new MapFunction<Tuple1<Long>, Object>() {
            @Override
            public Object map(Tuple1 tuple1) throws Exception {
                System.out.println("current thread id:" + Thread.currentThread().getId() + " value:" + tuple1);
                return tuple1.getField(0);
            }
        });

        result.print().setParallelism(1);

        executionEnvironment.execute("CustomPartitionDemo");
    }
}
