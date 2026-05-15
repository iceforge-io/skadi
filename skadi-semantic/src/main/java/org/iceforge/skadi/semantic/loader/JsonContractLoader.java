package org.iceforge.skadi.semantic.loader;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iceforge.skadi.semantic.contract.SemanticContract;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * {@link ContractLoader} implementation that reads JSON files using Jackson.
 *
 * <p>Jackson 2.14+ deserializes Java records natively via {@code RecordComponent.getName()};
 * no {@code @JsonProperty} annotations or {@code --parameters} compiler flags are required.
 * {@code AUTO_DETECT_IS_GETTERS} is disabled so that {@code SemanticAccessPolicy.isEmpty()}
 * is not misidentified as a {@code "empty"} JSON property.
 *
 * <p>Obtain an instance via {@link #create()}. The returned instance is thread-safe
 * after construction — {@link ObjectMapper} is safe for concurrent reads once configured.
 *
 * <p>This class has no Spring dependencies and does not require a Spring application
 * context. It does not populate {@code ContractRegistry}, execute SQL, call Databricks,
 * call S3, or enforce access policy.
 */
public final class JsonContractLoader implements ContractLoader {

    private final ObjectMapper mapper;

    private JsonContractLoader(ObjectMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Creates a pre-configured loader with the correct Jackson settings for
     * {@link SemanticContract} deserialization.
     */
    public static JsonContractLoader create() {
        return new JsonContractLoader(
                new ObjectMapper().disable(MapperFeature.AUTO_DETECT_IS_GETTERS));
    }

    @Override
    public SemanticContract load(Path path) {
        Objects.requireNonNull(path, "path must not be null");
        try (InputStream in = Files.newInputStream(path)) {
            return fromStream(in, path.toString());
        } catch (ContractLoadException e) {
            throw e;
        } catch (IOException e) {
            throw new ContractLoadException(path.toString(),
                    "cannot open contract file: " + e.getMessage(), e);
        }
    }

    @Override
    public SemanticContract load(InputStream inputStream) {
        Objects.requireNonNull(inputStream, "inputStream must not be null");
        return fromStream(inputStream, "<stream>");
    }

    @Override
    public List<SemanticContract> loadAll(List<Path> paths) {
        Objects.requireNonNull(paths, "paths must not be null");
        var results = new ArrayList<SemanticContract>(paths.size());
        for (var path : paths) {
            results.add(load(path));
        }
        return List.copyOf(results);
    }

    private SemanticContract fromStream(InputStream in, String sourceName) {
        try {
            return mapper.readValue(in, SemanticContract.class);
        } catch (JsonProcessingException e) {
            throw new ContractLoadException(sourceName,
                    "contract parse failed: " + e.getOriginalMessage(), e);
        } catch (IOException e) {
            throw new ContractLoadException(sourceName,
                    "cannot read contract: " + e.getMessage(), e);
        }
    }
}
