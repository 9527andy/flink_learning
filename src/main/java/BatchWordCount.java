import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;


public class BatchWordCount {
    public static void main(String[] args) throws Exception{
        final String inputPath = "/Users/zhangyi/flink/flink_learning/src/main/resources/inputFile";
        final  String outFilePath="/Users/zhangyi/flink/flink_learning/src/main/resources/outFile";
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> dataSource = executionEnvironment.readTextFile(inputPath);
        DataSet<Tuple2<String, Integer>> dataSet = dataSource.flatMap(new Token()).groupBy(0).sum(1);

        dataSet.writeAsCsv(outFilePath,"\n"," ", FileSystem.WriteMode.OVERWRITE);

        executionEnvironment.setParallelism(1);
        executionEnvironment.execute("BatchWordCount");
    }

    private static class Token implements FlatMapFunction<String, Tuple2<String, Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] split = s.toLowerCase().split("\n");
            for (String s1 : split) {
                if(s1.length()>0){
                    collector.collect(new Tuple2<>(s1,1));
                }
            }
        }
    }
}
