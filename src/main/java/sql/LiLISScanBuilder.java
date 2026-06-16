package sql;

import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.GreaterThanOrEqual;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.LessThanOrEqual;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.IsNotNull;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;

import java.util.ArrayList;
import java.util.List;

public class LiLISScanBuilder implements ScanBuilder, SupportsPushDownFilters {
    private final String indexName;
    private final LiLISRange range = new LiLISRange();
    private Filter[] pushedFilters = new Filter[0];

    public LiLISScanBuilder(String indexName) {
        this.indexName = indexName;
    }

    @Override
    public Filter[] pushFilters(Filter[] filters) {
        List<Filter> pushed = new ArrayList<>();
        List<Filter> remaining = new ArrayList<>();
        for (Filter filter : filters) {
            if (pushFilter(filter)) {
                pushed.add(filter);
            } else if (!(filter instanceof IsNotNull)) {
                remaining.add(filter);
            }
        }
        pushedFilters = pushed.toArray(new Filter[0]);
        return remaining.toArray(new Filter[0]);
    }

    @Override
    public Filter[] pushedFilters() {
        return pushedFilters;
    }

    @Override
    public Scan build() {
        return new LiLISScan(indexName, range);
    }

    private boolean pushFilter(Filter filter) {
        if (filter instanceof And) {
            And and = (And) filter;
            return pushFilter(and.left()) && pushFilter(and.right());
        }
        if (filter instanceof GreaterThan) {
            GreaterThan gt = (GreaterThan) filter;
            return updateLower(gt.attribute(), gt.value());
        }
        if (filter instanceof GreaterThanOrEqual) {
            GreaterThanOrEqual gte = (GreaterThanOrEqual) filter;
            return updateLower(gte.attribute(), gte.value());
        }
        if (filter instanceof LessThan) {
            LessThan lt = (LessThan) filter;
            return updateUpper(lt.attribute(), lt.value());
        }
        if (filter instanceof LessThanOrEqual) {
            LessThanOrEqual lte = (LessThanOrEqual) filter;
            return updateUpper(lte.attribute(), lte.value());
        }
        if (filter instanceof EqualTo) {
            EqualTo eq = (EqualTo) filter;
            if (!isXY(eq.attribute()) || !(eq.value() instanceof Number)) {
                return false;
            }
            double value = ((Number) eq.value()).doubleValue();
            range.updateLower(eq.attribute(), value);
            range.updateUpper(eq.attribute(), value);
            return true;
        }
        return false;
    }

    private boolean updateLower(String attribute, Object value) {
        if (!isXY(attribute) || !(value instanceof Number)) {
            return false;
        }
        range.updateLower(attribute, ((Number) value).doubleValue());
        return true;
    }

    private boolean updateUpper(String attribute, Object value) {
        if (!isXY(attribute) || !(value instanceof Number)) {
            return false;
        }
        range.updateUpper(attribute, ((Number) value).doubleValue());
        return true;
    }

    private boolean isXY(String attribute) {
        return "x".equalsIgnoreCase(attribute) || "y".equalsIgnoreCase(attribute);
    }
}
