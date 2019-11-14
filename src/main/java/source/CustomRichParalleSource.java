package source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class CustomRichParalleSource extends RichParallelSourceFunction<Long> {
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

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("RichParallelSourceFunction->open");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("RichParallelSourceFunction->close");
    }
}
