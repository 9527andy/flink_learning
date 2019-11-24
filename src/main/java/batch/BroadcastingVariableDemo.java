package batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BroadcastingVariableDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<Tuple2<String, Integer>> broadcastingData = new ArrayList<>();
        broadcastingData.add(new Tuple2<>("Julian", new Integer(31)));
        broadcastingData.add(new Tuple2<>("Amy", new Integer(30)));

        DataSet<Tuple2<String, Integer>> broadcastDataSet = env.fromCollection(broadcastingData);
        broadcastDataSet.map(new MapFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
            @Override
            public Map<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                Map<String, Integer> mapResult = new HashMap<>();
                mapResult.put(stringIntegerTuple2.f0, stringIntegerTuple2.f1);
                return mapResult;
            }
        });

        DataSource<String> sourceData = env.fromElements("Julian", "Amy");

        DataSet<String> broadcastSet = sourceData.map(new RichMapFunction<String, String>() {
            List<Tuple2<String,Integer>> broadcastVariable = new ArrayList<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                broadcastVariable=getRuntimeContext().getBroadcastVariable("broadcastingData");
                System.out.println("broadcasting variable:"+broadcastVariable);
            }

            @Override
            public String map(String s) throws Exception {
                StringBuilder sb = new StringBuilder(s);
                for (Tuple2<String, Integer> tuple2 : broadcastVariable) {
                    if(tuple2.f0.equals(s)) return sb.append(", age:"+tuple2.f1).toString();
                }
                return sb.toString();
            }
        }).withBroadcastSet(broadcastDataSet, "broadcastingData");

        broadcastSet.print();

       // env.execute("BroadcastingVariableDemo");
    }
}
