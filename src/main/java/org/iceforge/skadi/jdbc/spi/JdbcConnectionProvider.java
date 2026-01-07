package org.iceforge.skadi.jdbc.spi;

import org.iceforge.skadi.jdbc.SkadiJdbcProperties;

import java.sql.Connection;

/**
 * Pluggable JDBC connection provider.
 * <p>
 * Corporate deployments can implement this SPI to support custom authentication
 * flows (Kerberos, OAuth, federated cloud identity, proprietary ticketing, etc.)
 * without Skadi core needing to know the details.
 */
public interface JdbcConnectionProvider {

    /** A stable provider ID (e.g. "default", "corp-kerberos", "corp-oauth"). */
    String id();

    /** Return true if this provider should handle the given context. */
    boolean supports(JdbcClientContext context);

    /** Open a JDBC connection for the configured datasource. */
    Connection openConnection(JdbcClientContext context, SkadiJdbcProperties.JdbcDatasourceConfig cfg) throws Exception;
}
