package com.dkay229.skadi.aws.s3;

import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.netty.http.client.HttpClient;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

@Component
public class PeerCacheClient {

    private final WebClient webClient;

    public PeerCacheClient(WebClient.Builder builder) {
        HttpClient httpClient = HttpClient.create().compress(true);
        this.webClient = builder.clientConnector(new ReactorClientHttpConnector(httpClient)).build();
    }

    public Optional<Long> headLength(String peerBaseUrl, String bucket, String key,
                                     PeerSignedHeaders signed, Duration timeout) {
        try {
            return webClient.head()
                    .uri(peerBaseUrl + "/internal/cache/object?bucket={b}&key={k}", bucket, key)
                    .headers(h -> signed.apply(h))
                    .retrieve()
                    .toBodilessEntity()
                    .timeout(timeout)
                    .map(resp -> {
                        String cl = resp.getHeaders().getFirst(HttpHeaders.CONTENT_LENGTH);
                        return cl == null ? null : Long.parseLong(cl);
                    })
                    .blockOptional();
        } catch (Exception e) {
            return Optional.empty();
        }
    }

    /** Stream GET body directly into destTmp */
    public boolean streamToFile(String peerBaseUrl, String bucket, String key,
                                PeerSignedHeaders signed, Path destTmp, Duration timeout) {
        try {
            Flux<DataBuffer> body = webClient.get()
                    .uri(peerBaseUrl + "/internal/cache/object?bucket={b}&key={k}", bucket, key)
                    .headers(h -> signed.apply(h))
                    .retrieve()
                    .bodyToFlux(DataBuffer.class)
                    .timeout(timeout);

            DataBufferUtils.write(body, destTmp).then().block(timeout);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
