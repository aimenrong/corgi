package corgi.hub.core.mqtt.common;

import org.springframework.stereotype.Component;

import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Created by Terry LIANG on 2017/1/3.
 */
@Component
public class KafkaConsumerRejectedExecutionHandler implements RejectedExecutionHandler {
    @Override
    public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        int coreSize = executor.getCorePoolSize();
        int maxPoolSize = executor.getMaximumPoolSize();
        executor.setCorePoolSize(coreSize >> 2);
        executor.setMaximumPoolSize(maxPoolSize >> 2);
    }
}
