// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.catalog;

import org.apache.doris.analysis.IndexDef;
import org.apache.doris.analysis.InvertedIndexUtil;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.SqlUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TIndexType;
import org.apache.doris.thrift.TOlapTableIndex;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Internal representation of index, including index type, name, columns and comments.
 * This class will be used in olap table
 */
public class Index implements Writable {
    public static final int INDEX_ID_INIT_VALUE = -1;

    @SerializedName(value = "i", alternate = {"indexId"})
    private long indexId = -1; // -1 for compatiable
    @SerializedName(value = "in", alternate = {"indexName"})
    private String indexName;
    @SerializedName(value = "c", alternate = {"columns"})
    private List<String> columns;
    @SerializedName(value = "it", alternate = {"indexType"})
    private IndexDef.IndexType indexType;
    @SerializedName(value = "pt", alternate = {"properties"})
    private Map<String, String> properties;
    @SerializedName(value = "ct", alternate = {"comment"})
    private String comment;

    public Index(long indexId, String indexName, List<String> columns,
            IndexDef.IndexType indexType, Map<String, String> properties, String comment) {
        this.indexId = indexId;
        this.indexName = indexName;
        this.columns = columns == null ? Lists.newArrayList() : Lists.newArrayList(columns);
        this.indexType = indexType;
        this.properties = properties == null ? Maps.newHashMap() : Maps.newHashMap(properties);
        this.comment = comment;
        if (indexType == IndexDef.IndexType.INVERTED) {
            if (this.properties != null && !this.properties.isEmpty()) {
                if (this.properties.containsKey(InvertedIndexUtil.INVERTED_INDEX_PARSER_KEY)) {
                    String lowerCaseKey = InvertedIndexUtil.INVERTED_INDEX_PARSER_LOWERCASE_KEY;
                    if (!properties.containsKey(lowerCaseKey)) {
                        this.properties.put(lowerCaseKey, "true");
                    }
                    String supportPhraseKey = InvertedIndexUtil
                            .INVERTED_INDEX_SUPPORT_PHRASE_KEY;
                    if (!properties.containsKey(supportPhraseKey)) {
                        this.properties.put(supportPhraseKey, "true");
                    }
                }
            }
        }
    }

    public Index() {
        this.indexName = null;
        this.columns = null;
        this.indexType = null;
        this.properties = null;
        this.comment = null;
    }

    public long getIndexId() {
        return indexId;
    }

    public void setIndexId(long indexId) {
        this.indexId = indexId;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }

    public IndexDef.IndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(IndexDef.IndexType indexType) {
        this.indexType = indexType;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public String getPropertiesString() {
        if (properties == null || properties.isEmpty()) {
            return "";
        }

        // Use TreeMap to ensure consistent ordering of properties
        return "(" + new PrintableMap(new java.util.TreeMap<>(properties), "=", true, false, ",").toString() + ")";
    }

    public String getInvertedIndexParser() {
        return InvertedIndexUtil.getInvertedIndexParser(properties);
    }

    public boolean isInvertedIndexParserNone() {
        return InvertedIndexUtil.INVERTED_INDEX_PARSER_NONE.equals(getInvertedIndexParser());
    }

    public String getInvertedIndexParserMode() {
        return InvertedIndexUtil.getInvertedIndexParserMode(properties);
    }

    public Map<String, String> getInvertedIndexCharFilter() {
        return InvertedIndexUtil.getInvertedIndexCharFilter(properties);
    }

    public boolean getInvertedIndexParserLowercase() {
        return InvertedIndexUtil.getInvertedIndexParserLowercase(properties);
    }

    public String getInvertedIndexParserStopwords() {
        return InvertedIndexUtil.getInvertedIndexParserStopwords(properties);
    }

    // Whether the index can be changed in light mode
    public boolean isLightIndexChangeSupported() {
        return indexType == IndexDef.IndexType.INVERTED;
    }

    // Whether the index can be added in light mode
    // cloud mode supports light add for ngram_bf index and non-tokenized inverted index (parser="none")
    // local mode supports light add for both inverted index and ngram_bf index
    // the rest of the index types do not support light add
    public boolean isLightAddIndexSupported(boolean enableAddIndexForNewData) {
        if (Config.isCloudMode()) {
            if (indexType == IndexDef.IndexType.INVERTED) {
                return isInvertedIndexParserNone() && enableAddIndexForNewData;
            } else if (indexType == IndexDef.IndexType.NGRAM_BF) {
                return enableAddIndexForNewData;
            }
            return false;
        }
        return (indexType == IndexDef.IndexType.NGRAM_BF && enableAddIndexForNewData)
                || (indexType == IndexDef.IndexType.INVERTED);
    }

    public String getInvertedIndexCustomAnalyzer() {
        return InvertedIndexUtil.getInvertedIndexCustomAnalyzer(properties);
    }

    public String getComment() {
        return getComment(false);
    }

    public String getComment(boolean escapeQuota) {
        if (!escapeQuota) {
            return comment;
        }
        return SqlUtils.escapeQuota(comment);
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, GsonUtils.GSON.toJson(this));
    }

    public static Index read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Index.class);
    }

    @Override
    public int hashCode() {
        return 31 * (indexName.hashCode() + columns.hashCode() + indexType.hashCode());
    }

    public Index clone() {
        return new Index(indexId, indexName, new ArrayList<>(columns),
                indexType, new HashMap<>(properties), comment);
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder("INDEX ");
        sb.append(indexName);
        sb.append(" (");
        boolean first = true;
        for (String col : columns) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append("`" + col + "`");
        }
        sb.append(")");
        if (indexType != null) {
            sb.append(" USING ").append(indexType.toString());
        }
        if (properties != null && properties.size() > 0) {
            sb.append(" PROPERTIES");
            sb.append(getPropertiesString());
        }
        if (StringUtils.isNotBlank(comment)) {
            sb.append(" COMMENT '").append(getComment(true)).append("'");
        }
        return sb.toString();
    }

    public List<Integer> getColumnUniqueIds(List<Column> schema) {
        List<Integer> columnUniqueIds = new ArrayList<>();
        if (schema != null) {
            for (String columnName : columns) {
                for (Column column : schema) {
                    if (columnName.equalsIgnoreCase(column.getName())) {
                        columnUniqueIds.add(column.getUniqueId());
                    }
                }
            }
        }
        return columnUniqueIds;
    }

    public TOlapTableIndex toThrift(List<Integer> indexColumnUniqueIds) {
        TOlapTableIndex tIndex = new TOlapTableIndex();
        tIndex.setIndexId(indexId);
        tIndex.setIndexName(indexName);
        tIndex.setColumns(columns);
        tIndex.setIndexType(TIndexType.valueOf(indexType.toString()));
        if (properties != null) {
            tIndex.setProperties(properties);
        }
        tIndex.setColumnUniqueIds(indexColumnUniqueIds);
        return tIndex;
    }

    public OlapFile.TabletIndexPB toPb(Map<Integer, Column> columnMap, List<Integer> indexColumnUniqueIds) {
        OlapFile.TabletIndexPB.Builder builder = OlapFile.TabletIndexPB.newBuilder();
        builder.setIndexId(indexId);
        builder.setIndexName(indexName);

        for (Integer columnUniqueId : indexColumnUniqueIds) {
            Column column = columnMap.get(columnUniqueId);
            if (column != null) {
                builder.addColUniqueId(column.getUniqueId());
            }
        }

        switch (indexType) {
            case BITMAP:
                builder.setIndexType(OlapFile.IndexType.BITMAP);
                break;

            case INVERTED:
                builder.setIndexType(OlapFile.IndexType.INVERTED);
                break;

            case NGRAM_BF:
                builder.setIndexType(OlapFile.IndexType.NGRAM_BF);
                break;

            case BLOOMFILTER:
                builder.setIndexType(OlapFile.IndexType.BLOOMFILTER);
                break;

            default:
                throw new RuntimeException("indexType " + indexType + " is not processed in toPb");
        }

        if (properties != null) {
            builder.putAllProperties(properties);
        }

        OlapFile.TabletIndexPB index = builder.build();
        return index;
    }

    public static void checkConflict(Collection<Index> indices, Set<String> bloomFilters) throws AnalysisException {
        indices = indices == null ? Collections.emptyList() : indices;
        bloomFilters = bloomFilters == null ? Collections.emptySet() : bloomFilters;
        Set<String> bfColumns = new HashSet<>();
        for (Index index : indices) {
            if (IndexDef.IndexType.NGRAM_BF == index.getIndexType()
                    || IndexDef.IndexType.BLOOMFILTER == index.getIndexType()) {
                for (String column : index.getColumns()) {
                    column = column.toLowerCase();
                    if (bfColumns.contains(column)) {
                        throw new AnalysisException(column + " should have only one ngram bloom filter index or bloom "
                                + "filter index");
                    }
                    bfColumns.add(column);
                }
            }
        }
        for (String column : bloomFilters) {
            column = column.toLowerCase();
            if (bfColumns.contains(column)) {
                throw new AnalysisException(column + " should have only one ngram bloom filter index or bloom "
                        + "filter index");
            }
            bfColumns.add(column);
        }
    }
}
