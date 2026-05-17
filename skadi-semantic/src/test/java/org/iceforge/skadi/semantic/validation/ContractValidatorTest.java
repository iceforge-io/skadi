package org.iceforge.skadi.semantic.validation;

import org.iceforge.skadi.semantic.contract.SemanticAccessPolicy;
import org.iceforge.skadi.semantic.contract.SemanticCachePolicy;
import org.iceforge.skadi.semantic.contract.SemanticCacheStrategy;
import org.iceforge.skadi.semantic.contract.SemanticContract;
import org.iceforge.skadi.semantic.contract.SemanticContractVersion;
import org.iceforge.skadi.semantic.contract.SemanticDimension;
import org.iceforge.skadi.semantic.contract.SemanticEndpoint;
import org.iceforge.skadi.semantic.contract.SemanticEntity;
import org.iceforge.skadi.semantic.contract.SemanticFieldType;
import org.iceforge.skadi.semantic.contract.SemanticMeasure;
import org.iceforge.skadi.semantic.contract.SemanticRuleRef;
import org.iceforge.skadi.semantic.loader.JsonContractLoader;
import org.iceforge.skadi.semantic.query.SemanticFormatHint;
import org.iceforge.skadi.semantic.query.SemanticOutputColumn;
import org.iceforge.skadi.semantic.query.SemanticOutputRole;
import org.iceforge.skadi.semantic.query.SemanticOutputShape;
import org.iceforge.skadi.semantic.query.SemanticQueryContract;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.iceforge.skadi.semantic.validation.ContractValidationIssue.*;
import static org.iceforge.skadi.semantic.validation.ContractValidationSeverity.ERROR;
import static org.iceforge.skadi.semantic.validation.ContractValidationSeverity.WARNING;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link SemanticContractValidator}.
 *
 * <p>No Spring context, no network, no Databricks, no SQL execution,
 * no registry population.
 */
class ContractValidatorTest {

    private SemanticContractValidator validator;

    @BeforeEach
    void setUp() {
        validator = SemanticContractValidator.create();
    }

    // ── Sample fixture ─────────────────────────────────────────────────────────

    @Test
    void validate_sampleFixture_noErrors() {
        var loader   = JsonContractLoader.create();
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-contract.json")) {
            assertNotNull(in, "fixture not found");
            var contract = loader.load(in);
            var result   = validator.validate(contract);
            assertFalse(result.hasErrors(), "sample-contract.json should produce no errors");
        } catch (Exception e) {
            fail("unexpected exception: " + e.getMessage());
        }
    }

    @Test
    void validate_sampleFixture_noWarnings() {
        var loader = JsonContractLoader.create();
        try (InputStream in = getClass().getResourceAsStream("/fixtures/sample-contract.json")) {
            assertNotNull(in, "fixture not found");
            var result = validator.validate(loader.load(in));
            assertFalse(result.hasWarnings(), "sample-contract.json should produce no warnings");
            assertTrue(result.isValid());
        } catch (Exception e) {
            fail("unexpected exception: " + e.getMessage());
        }
    }

    // ── ContractValidationResult behaviour ────────────────────────────────────

    @Test
    void result_valid_hasNoIssues() {
        var r = ContractValidationResult.valid();
        assertTrue(r.issues().isEmpty());
        assertTrue(r.isValid());
        assertFalse(r.hasErrors());
        assertFalse(r.hasWarnings());
    }

    @Test
    void result_issuesListIsUnmodifiable() {
        var r = validator.validate(contract("c", measures("m"), dimensions("d")));
        assertThrows(UnsupportedOperationException.class, () -> r.issues().add(null));
    }

    @Test
    void result_isValid_trueWhenNoErrors() {
        var r = validator.validate(contract("c", measures("m"), dimensions("d")));
        assertTrue(r.isValid());
        assertFalse(r.hasErrors());
    }

    @Test
    void result_isValid_trueWhenOnlyWarnings() {
        // empty measures → warning only
        var r = validator.validate(contract("c", List.of(), dimensions("d")));
        assertTrue(r.isValid(), "warnings alone must not make result invalid");
        assertTrue(r.hasWarnings());
        assertFalse(r.hasErrors());
    }

    @Test
    void result_isValid_falseWhenErrors() {
        // two measures with the same name → error
        var r = validator.validate(contract("c",
                List.of(measure("dup"), measure("dup")), dimensions("d")));
        assertFalse(r.isValid());
        assertTrue(r.hasErrors());
    }

    // ── Duplicate measure names ────────────────────────────────────────────────

    @Test
    void validate_duplicateMeasureNames_returnsError() {
        var result = validator.validate(
                contract("c", List.of(measure("pnl"), measure("delta"), measure("pnl")),
                        dimensions("book")));
        assertTrue(result.hasErrors());
        var issue = singleIssue(result, ERROR, CONTRACT_DUPLICATE_MEASURE);
        assertEquals("c",   issue.contractName());
        assertEquals("measures", issue.location());
        assertTrue(issue.message().contains("'pnl'"));
    }

    @Test
    void validate_multipleDuplicateMeasures_allReported() {
        var result = validator.validate(
                contract("c",
                        List.of(measure("a"), measure("b"), measure("a"), measure("b")),
                        dimensions("d")));
        var codes = result.issues().stream()
                .filter(i -> i.code().equals(CONTRACT_DUPLICATE_MEASURE))
                .toList();
        assertEquals(2, codes.size(), "one ERROR per duplicated name");
    }

    @Test
    void validate_uniqueMeasureNames_noMeasureError() {
        var result = validator.validate(
                contract("c", measures("pnl", "delta"), dimensions("book")));
        assertTrue(result.issues().stream()
                .noneMatch(i -> i.code().equals(CONTRACT_DUPLICATE_MEASURE)));
    }

    // ── Duplicate dimension names ──────────────────────────────────────────────

    @Test
    void validate_duplicateDimensionNames_returnsError() {
        var result = validator.validate(
                contract("c", measures("pnl"),
                        List.of(dimension("book"), dimension("desk"), dimension("book"))));
        assertTrue(result.hasErrors());
        var issue = singleIssue(result, ERROR, CONTRACT_DUPLICATE_DIMENSION);
        assertEquals("c",          issue.contractName());
        assertEquals("dimensions", issue.location());
        assertTrue(issue.message().contains("'book'"));
    }

    @Test
    void validate_uniqueDimensionNames_noDimensionError() {
        var result = validator.validate(
                contract("c", measures("pnl"), dimensions("book", "desk")));
        assertTrue(result.issues().stream()
                .noneMatch(i -> i.code().equals(CONTRACT_DUPLICATE_DIMENSION)));
    }

    // ── Empty measures / dimensions ────────────────────────────────────────────

    @Test
    void validate_noMeasures_returnsWarning() {
        var result = validator.validate(contract("c", List.of(), dimensions("d")));
        var issue  = singleIssue(result, WARNING, CONTRACT_NO_MEASURES);
        assertEquals("c",        issue.contractName());
        assertEquals("measures", issue.location());
    }

    @Test
    void validate_noDimensions_returnsWarning() {
        var result = validator.validate(contract("c", measures("m"), List.of()));
        var issue  = singleIssue(result, WARNING, CONTRACT_NO_DIMENSIONS);
        assertEquals("c",          issue.contractName());
        assertEquals("dimensions", issue.location());
    }

    @Test
    void validate_emptyMeasuresAndDimensions_returnsTwoWarnings() {
        var result = validator.validate(contract("c", List.of(), List.of()));
        assertEquals(2, result.issues().stream()
                .filter(i -> i.severity() == WARNING).count());
    }

    // ── Cache policy warnings ──────────────────────────────────────────────────

    @Test
    void validate_datasetVersionStrategy_returnsWarning() {
        var c = contractWithCachePolicy("c",
                new SemanticCachePolicy(SemanticCacheStrategy.DATASET_VERSION, null, List.of()));
        var result = validator.validate(c);
        var issue  = singleIssue(result, WARNING, CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED);
        assertEquals("c",                    issue.contractName());
        assertEquals("cachePolicy.strategy", issue.location());
    }

    @Test
    void validate_ttlStrategy_noCacheWarning() {
        var c = contractWithCachePolicy("c", SemanticCachePolicy.ttl(300L));
        var result = validator.validate(c);
        assertTrue(result.issues().stream()
                .noneMatch(i -> i.code().equals(CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED)));
    }

    @Test
    void validate_noneStrategy_noCacheWarning() {
        var c = contractWithCachePolicy("c", SemanticCachePolicy.none());
        var result = validator.validate(c);
        assertTrue(result.issues().stream()
                .noneMatch(i -> i.code().equals(CONTRACT_CACHE_DATASET_VERSION_UNSUPPORTED)));
    }

    // ── validateAll — cross-contract ───────────────────────────────────────────

    @Test
    void validateAll_distinctContracts_noErrors() {
        var result = validator.validateAll(List.of(
                contract("alpha", measures("m"), dimensions("d")),
                contract("beta",  measures("m"), dimensions("d"))));
        assertFalse(result.hasErrors());
    }

    @Test
    void validateAll_duplicateContractNames_returnsError() {
        var result = validator.validateAll(List.of(
                contract("shared", measures("m"), dimensions("d")),
                contract("shared", measures("n"), dimensions("e"))));
        var issue = singleIssue(result, ERROR, DUPLICATE_CONTRACT_NAME);
        assertEquals("shared", issue.contractName());
        assertNull(issue.location());
        assertTrue(issue.message().contains("'shared'"));
    }

    @Test
    void validateAll_triplicateContractName_reportsOnce() {
        var result = validator.validateAll(List.of(
                contract("dup", measures("m"), dimensions("d")),
                contract("dup", measures("m"), dimensions("d")),
                contract("dup", measures("m"), dimensions("d"))));
        long count = result.issues().stream()
                .filter(i -> i.code().equals(DUPLICATE_CONTRACT_NAME)).count();
        assertEquals(1, count, "should report each duplicate name exactly once");
    }

    @Test
    void validateAll_emptyList_returnsValid() {
        var result = validator.validateAll(List.of());
        assertTrue(result.isValid());
        assertTrue(result.issues().isEmpty());
    }

    @Test
    void validateAll_crossContractIssuesComeLast() {
        // alpha has a duplicate measure; both share a name (cross-contract issue)
        var alpha = contract("shared",
                List.of(measure("dup"), measure("dup")), dimensions("d"));
        var beta  = contract("shared", measures("m"), dimensions("d"));

        var result = validator.validateAll(List.of(alpha, beta));
        var issues = result.issues();
        assertFalse(issues.isEmpty());

        // The per-contract duplicate measure issue should appear before the cross-contract one
        int measureIdx  = indexOfCode(issues, CONTRACT_DUPLICATE_MEASURE);
        int contractIdx = indexOfCode(issues, DUPLICATE_CONTRACT_NAME);
        assertTrue(measureIdx < contractIdx,
                "per-contract issues should precede cross-contract issues");
    }

    @Test
    void validateAll_nullElementInList_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> validator.validateAll(List.of(
                        contract("ok", measures("m"), dimensions("d")),
                        (SemanticContract) null)));
    }

    // ── validateAll: result immutability ──────────────────────────────────────

    @Test
    void validateAll_issuesListIsUnmodifiable() {
        var result = validator.validateAll(List.of(
                contract("a", measures("m"), dimensions("d"))));
        assertThrows(UnsupportedOperationException.class, () -> result.issues().add(null));
    }

    // ── SemanticQueryContract validation ──────────────────────────────────────

    @Test
    void validate_queryContract_noDuplicateColumns_noErrors() {
        var qc     = queryContract("qc", "mxl_risk", cols("pnl", "book"));
        var result = validator.validate(qc);
        assertFalse(result.hasErrors());
    }

    @Test
    void validate_queryContract_duplicateOutputColumns_returnsError() {
        var qc     = queryContract("qc", "mxl_risk", cols("pnl", "pnl"));
        var result = validator.validate(qc);
        var issue  = singleIssue(result, ERROR, QUERY_CONTRACT_DUPLICATE_OUTPUT_COLUMN);
        assertEquals("qc",                   issue.contractName());
        assertEquals("outputShape.columns",  issue.location());
        assertTrue(issue.message().contains("'pnl'"));
    }

    // ── Cross-reference validation ─────────────────────────────────────────────

    @Test
    void validateAll_crossRef_resolvedSource_noUnresolvedError() {
        var contracts = List.of(contract("mxl_risk", measures("pnl"), dimensions("book")));
        var queryContracts = List.of(queryContract("pnl_by_book", "mxl_risk", cols("pnl")));
        var result = validator.validateAll(contracts, queryContracts);
        assertTrue(result.issues().stream()
                .noneMatch(i -> i.code().equals(QUERY_CONTRACT_UNRESOLVED_SOURCE)));
    }

    @Test
    void validateAll_crossRef_unresolvedSource_returnsError() {
        var contracts = List.of(contract("mxl_risk", measures("pnl"), dimensions("book")));
        var queryContracts = List.of(queryContract("bad_qc", "non_existent", cols("pnl")));
        var result = validator.validateAll(contracts, queryContracts);
        var issue  = singleIssue(result, ERROR, QUERY_CONTRACT_UNRESOLVED_SOURCE);
        assertEquals("bad_qc",       issue.contractName());
        assertEquals("sourceContract", issue.location());
        assertTrue(issue.message().contains("'non_existent'"));
    }

    @Test
    void validateAll_crossRef_nullQueryContractList_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> validator.validateAll(List.of(), null));
    }

    // ── Determinism ────────────────────────────────────────────────────────────

    @Test
    void validate_issueOrderingIsDeterministic() {
        var c = contract("c",
                List.of(measure("dup"), measure("dup")),
                List.of(dimension("dupD"), dimension("dupD")));
        var r1 = validator.validate(c);
        var r2 = validator.validate(c);
        assertEquals(r1.issues(), r2.issues(), "same input must produce identical issue list");
    }

    @Test
    void validateAll_issueOrderingIsDeterministic() {
        var contracts = List.of(
                contract("alpha", measures("m"), dimensions("d")),
                contract("alpha", measures("m"), dimensions("d")));
        var r1 = validator.validateAll(contracts);
        var r2 = validator.validateAll(contracts);
        assertEquals(r1.issues(), r2.issues());
    }

    // ── Null argument guards ───────────────────────────────────────────────────

    @Test
    void validate_nullContract_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> validator.validate((SemanticContract) null));
    }

    @Test
    void validate_nullQueryContract_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> validator.validate((SemanticQueryContract) null));
    }

    @Test
    void validateAll_nullList_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> validator.validateAll((List<SemanticContract>) null));
    }

    // ── ContractValidationIssue guards ────────────────────────────────────────

    @Test
    void issue_blankCode_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
                () -> new ContractValidationIssue(ERROR, "  ", "msg", "c", null));
    }

    @Test
    void issue_nullSeverity_throwsNullPointerException() {
        assertThrows(NullPointerException.class,
                () -> new ContractValidationIssue(null, "CODE", "msg", "c", null));
    }

    @Test
    void issue_nullLocation_isAllowed() {
        assertDoesNotThrow(
                () -> new ContractValidationIssue(WARNING, "CODE", "msg", "c", null));
    }

    // ── Helpers ────────────────────────────────────────────────────────────────

    private static SemanticContract contract(String name,
                                             List<SemanticMeasure> measures,
                                             List<SemanticDimension> dimensions) {
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity(name, "",
                        new SemanticEndpoint("c", "s", "t"),
                        List.of()),
                measures, dimensions,
                SemanticAccessPolicy.unrestricted(),
                SemanticCachePolicy.none(),
                null);
    }

    private static SemanticContract contractWithCachePolicy(String name, SemanticCachePolicy policy) {
        return new SemanticContract(
                name, new SemanticContractVersion("1.0.0"), "",
                new SemanticEntity(name, "",
                        new SemanticEndpoint("c", "s", "t"),
                        List.of(new SemanticRuleRef("R1", "r"))),
                measures("m"), dimensions("d"),
                SemanticAccessPolicy.unrestricted(), policy, null);
    }

    private static List<SemanticMeasure> measures(String... names) {
        return java.util.Arrays.stream(names).map(ContractValidatorTest::measure).toList();
    }

    private static List<SemanticDimension> dimensions(String... names) {
        return java.util.Arrays.stream(names).map(ContractValidatorTest::dimension).toList();
    }

    private static SemanticMeasure measure(String name) {
        return new SemanticMeasure(name, name, "SUM(" + name + ")",
                SemanticFieldType.DECIMAL, "", null);
    }

    private static SemanticDimension dimension(String name) {
        return new SemanticDimension(name, name, SemanticFieldType.STRING, name, true, true, null);
    }

    private static SemanticQueryContract queryContract(String name, String sourceContract,
                                                       List<SemanticOutputColumn> columns) {
        return new SemanticQueryContract(
                name, "", sourceContract,
                new SemanticContractVersion("1.0.0"),
                new SemanticOutputShape(columns),
                List.of());
    }

    private static List<SemanticOutputColumn> cols(String... names) {
        return java.util.Arrays.stream(names).map(ContractValidatorTest::col).toList();
    }

    private static SemanticOutputColumn col(String name) {
        return new SemanticOutputColumn(name, name, SemanticFieldType.DECIMAL, true,
                SemanticOutputRole.MEASURE, (SemanticFormatHint) null, List.of());
    }

    private static ContractValidationIssue singleIssue(ContractValidationResult result,
                                                        ContractValidationSeverity severity,
                                                        String code) {
        var matching = result.issues().stream()
                .filter(i -> i.severity() == severity && i.code().equals(code))
                .toList();
        assertEquals(1, matching.size(),
                "expected exactly one issue with severity=" + severity + " code=" + code
                        + "; got: " + result.issues());
        return matching.get(0);
    }

    private static int indexOfCode(List<ContractValidationIssue> issues, String code) {
        for (int i = 0; i < issues.size(); i++) {
            if (issues.get(i).code().equals(code)) return i;
        }
        return -1;
    }
}
