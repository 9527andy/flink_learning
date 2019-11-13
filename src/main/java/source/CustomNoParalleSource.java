package source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class CustomNoParalleSource implements SourceFunction<Long> {
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
