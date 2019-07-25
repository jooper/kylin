
package org.apache.kylin.job;

import static org.apache.kylin.job.util.FlatTableSqlQuoteUtils.quoteIdentifier;
import static org.apache.kylin.job.util.FlatTableSqlQuoteUtils.quoteIdentifierInSqlExpr;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.job.util.FlatTableSqlQuoteUtils;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.metadata.model.TblColRef;

//20190725 jwp  支持Oracle数据调整

public class JoinedFlatTableRefactor {

    //20190725  #针对Oracle调整生成的语法结构

    public static String generateSelectDataStatement(IJoinedFlatTableDesc flatDesc, boolean singleLine,
            String[] skipAs) {
        final String sep = singleLine ? " " : "\n";

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT" + sep);

        for (int i = 0; i < flatDesc.getAllColumns().size(); i++) {
            TblColRef col = flatDesc.getAllColumns().get(i);
            if (i > 0) {
                sql.append(",");
            }
            sql.append(getQuotedColExpressionInSourceDB(flatDesc, col)).append(sep);
        }
        appendJoinStatement(flatDesc, sql, singleLine);
        appendWhereStatement(flatDesc, sql, singleLine);
        return sql.toString();
    }

    public static String getQuotedColExpressionInSourceDB(IJoinedFlatTableDesc flatDesc, TblColRef col) {
        if (!col.getColumnDesc().isComputedColumn()) {
            return col.getName();
        } else {
            String computeExpr = col.getColumnDesc().getComputedColumnExpr();
            return quoteIdentifierInSqlExpr(flatDesc, computeExpr);
        }
    }

    static void appendJoinStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";
        Set<TableRef> dimTableCache = new HashSet<>();

        DataModelDesc model = flatDesc.getDataModel();
        TableRef rootTable = model.getRootFactTable();
        sql.append(" FROM ")
                .append(flatDesc.getDataModel().getRootFactTable()
                        .getTableIdentityQuoted(FlatTableSqlQuoteUtils.getQuote()))
//                .append(rootTable.getAlias())
                .append(sep);

        for (JoinTableDesc lookupDesc : model.getJoinTables()) {
            JoinDesc join = lookupDesc.getJoin();
            if (join != null && join.getType().equals("") == false) {
                TableRef dimTable = lookupDesc.getTableRef();
                if (!dimTableCache.contains(dimTable)) {
                    TblColRef[] pk = join.getPrimaryKeyColumns();
                    TblColRef[] fk = join.getForeignKeyColumns();
                    if (pk.length != fk.length) {
                        throw new RuntimeException("Invalid join condition of lookup table:" + lookupDesc);
                    }
                    String joinType = join.getType().toUpperCase(Locale.ROOT);

                    sql.append(joinType).append(" JOIN ")
                            .append(dimTable.getTableIdentityQuoted(FlatTableSqlQuoteUtils.getQuote()))
//                            .append(dimTable.getAlias())
                            .append(sep);
                    sql.append("ON ");
                    for (int i = 0; i < pk.length; i++) {
                        if (i > 0) {
                            sql.append(" AND ");
                        }
                        sql.append(getQuotedColExpressionInSourceDB(flatDesc, fk[i])).append(" = ")
                                .append(getQuotedColExpressionInSourceDB(flatDesc, pk[i]));
                    }
                    sql.append(sep);

                    dimTableCache.add(dimTable);
                }
            }
        }
    }

    private static void appendWhereStatement(IJoinedFlatTableDesc flatDesc, StringBuilder sql, boolean singleLine) {
        final String sep = singleLine ? " " : "\n";

        StringBuilder whereBuilder = new StringBuilder();
        whereBuilder.append("WHERE 1=1");

        DataModelDesc model = flatDesc.getDataModel();
        if (StringUtils.isNotEmpty(model.getFilterCondition())) {
            String quotedFilterCondition = quoteIdentifierInSqlExpr(flatDesc, model.getFilterCondition());
            whereBuilder.append(" AND (").append(quotedFilterCondition).append(") "); // -> filter condition contains special character may cause bug
        }
        if (flatDesc.getSegment() != null) {
            PartitionDesc partDesc = model.getPartitionDesc();
            if (partDesc != null && partDesc.getPartitionDateColumn() != null) {
                SegmentRange segRange = flatDesc.getSegRange();

                if (segRange != null && !segRange.isInfinite()) {
                    whereBuilder.append(" AND (");
                    String quotedPartitionCond = quoteIdentifierInSqlExpr(flatDesc,
                            partDesc.getPartitionConditionBuilder().buildDateRangeCondition(partDesc,
                                    flatDesc.getSegment(), segRange));
                    whereBuilder.append(quotedPartitionCond);
                    whereBuilder.append(")" + sep);
                }
            }
        }

        sql.append(whereBuilder.toString());
    }

}
