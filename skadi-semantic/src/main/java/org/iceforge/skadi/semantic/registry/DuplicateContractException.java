package org.iceforge.skadi.semantic.registry;

/**
 * Thrown by {@link ContractRegistry#register} when a contract with the same
 * name is already registered.
 *
 * <p>This is an unchecked exception — callers that are certain a contract name
 * is unique may ignore it, while callers that need to handle duplicates
 * (e.g., a hot-reload loader) can catch it explicitly.
 */
public class DuplicateContractException extends RuntimeException {

    private final String contractName;

    /**
     * @param contractName the name of the contract that was already registered
     */
    public DuplicateContractException(String contractName) {
        super("Contract already registered: '" + contractName + "'");
        this.contractName = contractName;
    }

    /** Returns the name of the conflicting contract. */
    public String contractName() {
        return contractName;
    }
}
