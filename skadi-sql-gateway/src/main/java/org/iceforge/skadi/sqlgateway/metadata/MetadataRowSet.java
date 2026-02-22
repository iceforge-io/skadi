package org.iceforge.skadi.sqlgateway.metadata;

import java.util.List;
import java.util.Objects;

/**
 * A tiny tabular result representation for answering pgwire metadata queries.
 *
 * <p>All values are returned as text for now, since the pgwire MVP only supports TEXT columns.
 */
public record MetadataRowSet(List<String> columns, List<List<String>> rows, String commandTag) {

    public MetadataRowSet {
        Objects.requireNonNull(columns, "columns");
        Objects.requireNonNull(rows, "rows");
        commandTag = (commandTag == null || commandTag.isBlank()) ? "SELECT 0" : commandTag;
    }

    public static MetadataRowSet of(List<String> columns, List<List<String>> rows) {
        return new MetadataRowSet(columns, rows, "SELECT " + rows.size());
    }
}

