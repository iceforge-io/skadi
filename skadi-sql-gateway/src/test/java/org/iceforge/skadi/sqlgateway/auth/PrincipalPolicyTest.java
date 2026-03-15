package org.iceforge.skadi.sqlgateway.auth;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class PrincipalPolicyTest {

    @Test
    void unrestrictedPermitsAnySchema() {
        assertThat(PrincipalPolicy.UNRESTRICTED.permitsSchema("sales")).isTrue();
        assertThat(PrincipalPolicy.UNRESTRICTED.permitsSchema("finance")).isTrue();
        assertThat(PrincipalPolicy.UNRESTRICTED.permitsSchema(null)).isTrue();
    }

    @Test
    void unrestrictedIsUnrestricted() {
        assertThat(PrincipalPolicy.UNRESTRICTED.isUnrestricted()).isTrue();
    }

    @Test
    void restrictedPermitsOnlyAllowedSchema() {
        PrincipalPolicy p = new PrincipalPolicy(List.of("sales", "finance"));
        assertThat(p.permitsSchema("sales")).isTrue();
        assertThat(p.permitsSchema("finance")).isTrue();
        assertThat(p.permitsSchema("hr")).isFalse();
    }

    @Test
    void schemaMatchIsCaseInsensitive() {
        PrincipalPolicy p = new PrincipalPolicy(List.of("Sales"));
        assertThat(p.permitsSchema("SALES")).isTrue();
        assertThat(p.permitsSchema("sales")).isTrue();
    }

    @Test
    void nullSchemaRejectedWhenRestricted() {
        PrincipalPolicy p = new PrincipalPolicy(List.of("sales"));
        assertThat(p.permitsSchema(null)).isFalse();
    }

    @Test
    void restrictedIsNotUnrestricted() {
        assertThat(new PrincipalPolicy(List.of("sales")).isUnrestricted()).isFalse();
    }
}
