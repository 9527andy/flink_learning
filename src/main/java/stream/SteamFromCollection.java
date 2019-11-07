package stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collection;

public class SteamFromCollection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        Collection<Integer> dataCollection = new ArrayList<Integer>();
        for (int i = 0; i < 10; i++) {
            dataCollection.add(i);
        }
        DataStreamSource<Integer> dataStreamSource = executionEnvironment.fromCollection(dataCollection);
        executionEnvironment.setParallelism(1);

//        DataStream<Object> dataStream = dataStreamSource.map(new MapFunction<Integer, Object>() {
//            @Override
//            public Object map(Integer integer) throws Exception {
//                return integer+1;
//            }
//        });

        DataStream<Object> dataStream = dataStreamSource.map(integer -> integer+1);

        dataStream.print();
        executionEnvironment.execute("SteamFromCollection");
    }
}
