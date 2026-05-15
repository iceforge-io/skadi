package org.iceforge.skadi.semantic.loader;

import org.iceforge.skadi.semantic.contract.SemanticContract;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;

/**
 * Loads {@link SemanticContract} instances from JSON files or streams.
 *
 * <p>Implementations parse the contract representation (JSON per ADR-011) and
 * return fully validated, immutable {@link SemanticContract} records. The loader
 * is responsible for parse and structural errors only — cross-contract validation
 * (duplicate names, broken references) is handled by the contract validator (D6).
 *
 * <p>Implementations must be:
 * <ul>
 *   <li><strong>Offline</strong> — no Databricks, S3, SQL Gateway, or cache calls</li>
 *   <li><strong>Stateless</strong> — no registry population; callers own the output list</li>
 *   <li><strong>Deterministic</strong> — same input always produces the same output</li>
 *   <li><strong>Spring-free</strong> — usable without a Spring application context</li>
 * </ul>
 *
 * <p>All methods throw {@link ContractLoadException} (unchecked) on any failure.
 * The exception message always includes the source path or stream label.
 */
public interface ContractLoader {

    /**
     * Loads a single contract from a filesystem path.
     *
     * @param path path to a {@code .json} contract file; must not be null
     * @return the parsed contract; never null
     * @throws ContractLoadException if the file cannot be opened, is not valid JSON,
     *                               or the JSON does not map to a valid contract
     */
    SemanticContract load(Path path);

    /**
     * Loads a single contract from an already-opened input stream.
     *
     * <p>The stream is read to completion but is <em>not</em> closed by this method —
     * the caller retains ownership.
     *
     * @param inputStream stream containing JSON contract data; must not be null
     * @return the parsed contract; never null
     * @throws ContractLoadException if the stream cannot be read, is not valid JSON,
     *                               or the JSON does not map to a valid contract
     */
    SemanticContract load(InputStream inputStream);

    /**
     * Loads contracts from a list of filesystem paths, preserving the list order.
     *
     * <p>Fails fast: if any path fails to load, a {@link ContractLoadException} is thrown
     * immediately and the remaining paths are not processed.
     *
     * @param paths ordered list of paths; must not be null; may be empty
     * @return unmodifiable list of parsed contracts in the same order as {@code paths};
     *         never null; empty if {@code paths} is empty
     * @throws ContractLoadException if any path fails to load
     */
    List<SemanticContract> loadAll(List<Path> paths);
}
