package partition;

import org.apache.flink.api.common.functions.Partitioner;

public class CustomPartition implements Partitioner<Long> {
    @Override
    public int partition(Long aLong, int i) {
        System.out.println("number of partition" + i);
        if (aLong % 2 == 0)
            return 1;
        else
            return 0;
    }
}
