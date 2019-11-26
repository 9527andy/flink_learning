package batch;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class CounterDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = executionEnvironment.fromElements("a", "b", "c", "d");

        DataSet<String> map = dataSource.map(new RichMapFunction<String, String>() {
            private IntCounter intSum = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("int-sum", intSum);
            }


            @Override
            public String map(String s) throws Exception {
                intSum.add(1);
                System.out.println("current accumulator value:"+intSum);
                return s;
            }
        }).setParallelism(4);

        map.writeAsText("./int-sum", FileSystem.WriteMode.OVERWRITE);
        JobExecutionResult jobExecutionResult = executionEnvironment.execute("CounterDemo");

        int sum = jobExecutionResult.getAccumulatorResult("int-sum");
        System.out.println("int-sum accumulator:"+sum);
    }
}
