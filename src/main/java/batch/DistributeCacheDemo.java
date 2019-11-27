package batch;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

public class DistributeCacheDemo {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.registerCachedFile("./src/main/resources/distributeCache","distributeCache");

        DataSource<String> dataSource = env.fromElements("a", "b", "c", "d");
        DataSet<String> result = dataSource.map(new RichMapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                return s;
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                File distributeCacheFile = getRuntimeContext().getDistributedCache().getFile("distributeCache");
                List<String> line = FileUtils.readLines(distributeCacheFile);
                for (String s : line) {
                    System.out.println("each line in cache: "+s);
                }
            }
        });

        result.print();
    }
}
