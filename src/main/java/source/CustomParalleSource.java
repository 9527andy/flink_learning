package source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class CustomParalleSource implements ParallelSourceFunction<Long> {
    private boolean isRunning = true;
    private Long count = 1L;

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(count);
            count++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
