package sql;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class LiLISTable implements Table, SupportsRead {
    static final StructType SCHEMA = DataTypes.createStructType(new StructField[]{
            DataTypes.createStructField("x", DataTypes.DoubleType, false),
            DataTypes.createStructField("y", DataTypes.DoubleType, false)
    });

    private final String indexName;

    public LiLISTable(Map<String, String> properties) {
        this.indexName = property(properties, "index_name", "name");
        if (this.indexName == null || this.indexName.trim().isEmpty()) {
            throw new IllegalArgumentException("LiLIS DataSource requires OPTIONS(index_name '...')");
        }
    }

    @Override
    public String name() {
        return "lilis." + indexName;
    }

    @Override
    public StructType schema() {
        return SCHEMA;
    }

    @Override
    public Set<TableCapability> capabilities() {
        return Collections.singleton(TableCapability.BATCH_READ);
    }

    @Override
    public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
        return new LiLISScanBuilder(indexName);
    }

    private static String property(Map<String, String> properties, String... names) {
        for (String name : names) {
            if (properties.containsKey(name)) {
                return properties.get(name);
            }
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                if (entry.getKey().equalsIgnoreCase(name)) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }
}
