package org.iceforge.skadi.query;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class QueryConfig {

    @Bean
    public ExecutorService queryExecutor(QueryCacheProperties props) {
        int threads = Math.max(1, props.getMaxConcurrentWrites());
        return Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "skadi-query");
            t.setDaemon(true);
            return t;
        });
    }
}
