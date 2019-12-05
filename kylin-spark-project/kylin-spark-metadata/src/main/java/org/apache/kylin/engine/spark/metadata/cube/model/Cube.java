/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.engine.spark.metadata.cube.model;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.BiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.engine.spark.metadata.cube.PathManager;
import org.apache.kylin.metadata.model.TblColRef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.kylin.common.persistence.ResourceStore.CUBE_RESOURCE_ROOT;
import static org.apache.kylin.metadata.MetadataConstants.FILE_SURFIX;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class Cube extends RootPersistentEntity {

    @JsonIgnore
    private KylinConfig config;

    @JsonProperty("projefct")
    private String project;

    @JsonProperty("name")
    private String cubeName;

    @JsonProperty("model")
    private DataModel dataModel;

    @JsonManagedReference
    @JsonProperty("segments")
    private List<DataSegment> segments = new ArrayList<>();

    private Cube(KylinConfig config) {}

    private transient SpanningTree spanningTree = null; // transient, because can self recreate

    @JsonProperty("indexes")
    private List<IndexEntity> indexEntities = new ArrayList<>();

    private List<DimensionDesc> dimensions = new ArrayList<>();

    private List<MeasureDesc> measures = new ArrayList<>();

    private transient BiMap<Integer, TblColRef> effectiveDimCols; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable
    private transient BiMap<Integer, MeasureDesc> effectiveMeasures; // BiMap impl (com.google.common.collect.Maps$FilteredEntryBiMap) is not serializable

    public static Cube getInstance(KylinConfig config) {
        return new Cube(config);
    }
  
    public static Cube getInstance(KylinConfig config, String uuid) {
        return new Cube(config);
    }
  
    //TODO[xyxy]: if exists another way to init
    private void initDimensionAndMeasures() {
        List<IndexEntity> indexes = getAllIndexes();
        int size1 = 1;
        int size2 = 1;
        for (IndexEntity cuboid : indexes) {
            size1 = Math.max(cuboid.getDimensionBitset().size(), size1);
            size2 = Math.max(cuboid.getMeasureBitset().size(), size2);
        }

        final BitSet dimBitSet = new BitSet(size1);
        final BitSet measureBitSet = new BitSet(size2);

        for (IndexEntity cuboid : indexes) {
            dimBitSet.or(cuboid.getDimensionBitset().mutable());
            measureBitSet.or(cuboid.getMeasureBitset().mutable());
        }

        this.effectiveDimCols = Maps.filterKeys(getModel().getEffectiveColsMap(),
                input -> input != null && dimBitSet.get(input));
        this.effectiveMeasures = Maps.filterKeys(getModel().getEffectiveMeasureMap(),
                input -> input != null && measureBitSet.get(input));
    }

    //add layout when build cube
    public Cube updateCube(IndexEntity[] indexes) {
        List<IndexEntity> indexEntities = Arrays.asList(indexes);
        this.addIndexEntities(indexEntities);
        return this;
    }

    public DataSegment getSegment(String segId) {
        for (DataSegment seg : segments) {
            if (seg.getId().equals(segId))
                return seg;
        }
        return null;
    }

    public BiMap<Integer, TblColRef> getEffectiveDimCols() {
        return effectiveDimCols;
    }

    public void setEffectiveDimCols(BiMap<Integer, TblColRef> effectiveDimCols) {
        this.effectiveDimCols = effectiveDimCols;
    }

    public BiMap<Integer, MeasureDesc> getEffectiveMeasures() {
        return effectiveMeasures;
    }

    public void setEffectiveMeasures(BiMap<Integer, MeasureDesc> effectiveMeasures) {
        this.effectiveMeasures = effectiveMeasures;
    }

    public DataSegment appendSegment(SegmentRange segmentRange) {
        DataSegment segment = new DataSegment(this, segmentRange);
        segments.add(segment);
        return segment;
    }
  
    public DataModel getModel() {
        return dataModel;
    }

    public void setDataModel(DataModel dataModel) {
        this.dataModel = dataModel;
    }

    public SpanningTree getSpanningTree() {
        if (spanningTree != null)
            return spanningTree;

        synchronized (this) {
            if (spanningTree == null) {
                spanningTree = SpanningTreeFactory.fromCube(this);
            }
            return spanningTree;
        }
    }

    public String getCubeName() {
        return cubeName;
    }

    public void setCubeName(String cubeName) {
        this.cubeName = cubeName;
    }

    public void setSpanningTree(SpanningTree spanningTree) {
        this.spanningTree = spanningTree;
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getId() {
        return uuid;
    }

    //used to dump resource before spark job submit
    public String getResourcePath() {
        return concatResourcePath(getUuid(), project);
    }

    public static String concatResourcePath(String name, String project) {
        return "/" + project + CUBE_RESOURCE_ROOT + "/" + name + FILE_SURFIX;
    }

    public List<LayoutEntity> getAllLayouts() {
        List<LayoutEntity> r = Lists.newArrayList();

        for (IndexEntity cd : getAllIndexes()) {
            r.addAll(cd.getLayouts());
        }
        return r;
    }

    public List<IndexEntity> getAllIndexes() {
//        Map<Long, Integer> retSubscriptMap = Maps.newHashMap();
//        List<IndexEntity> mergedIndexes = Lists.newArrayList();
//        int retSubscript = 0;
//        for (IndexEntity indexEntity : indexEntities) {
//            IndexEntity copy = JsonUtil.deepCopyQuietly(indexEntity, IndexEntity.class);
//            retSubscriptMap.put(indexEntity.getId(), retSubscript);
//            mergedIndexes.add(copy);
//            retSubscript++;
//        }

        //TODO: comment out for now
//        for (LayoutEntity ruleBasedLayout : ruleBasedLayouts) {
//            val ruleRelatedIndex = ruleBasedLayout.getIndex();
//            if (!retSubscriptMap.containsKey(ruleRelatedIndex.getId())) {
//                val copy = JsonUtil.deepCopyQuietly(ruleRelatedIndex, IndexEntity.class);
//                retSubscriptMap.put(ruleRelatedIndex.getId(), retSubscript);
//                mergedIndexes.add(copy);
//                retSubscript++;
//            }
//            val subscript = retSubscriptMap.get(ruleRelatedIndex.getId());
//            val targetIndex = mergedIndexes.get(subscript);
//            val originLayouts = targetIndex.getLayouts();
//            boolean isMatch = originLayouts.stream().filter(originLayout -> originLayout.equals(ruleBasedLayout))
//                    .peek(originLayout -> originLayout.setManual(true)).count() > 0;
//            LayoutEntity copyRuleBasedLayout;
//            copyRuleBasedLayout = JsonUtil.deepCopyQuietly(ruleBasedLayout, LayoutEntity.class);
//            if (!isMatch) {
//                originLayouts.add(copyRuleBasedLayout);
//            }
//            targetIndex.setLayouts(originLayouts);
//            targetIndex.setNextLayoutOffset(Math.max(targetIndex.getNextLayoutOffset(),
//                    originLayouts.stream().mapToLong(LayoutEntity::getId).max().orElse(0) % INDEX_ID_STEP + 1));
//            copyRuleBasedLayout.setIndex(targetIndex);
//        }

//        mergedIndexes.forEach(value -> value.setCube(this));
//        return mergedIndexes;
        return indexEntities;
    }

    public Set<String> collectPrecalculationResource() {
        Set<String> r = new LinkedHashSet<>();

        // cube & segments
        r.add(PathManager.getCubePath(project, getId()));

        // project & model & tables
        r.add(PathManager.getProjectPath(project));
        r.add(PathManager.getModelPath(project, getId()));
        for (TableRef t : getModel().getAllTableRefs()) {
            r.add(t.getTableDesc().getResourcePath());
        }

        return r;
    }

    public LayoutEntity getCuboidLayout(Long cuboidLayoutId) {
        return getSpanningTree().getCuboidLayout(cuboidLayoutId);
    }

    public KylinConfigExt getConfig() {
        return (KylinConfigExt) config;
    }

    public void setConfig(KylinConfig config) {
        this.config = config;
    }

    public List<IndexEntity> getIndexEntities() {
        return indexEntities;
    }

    public void setIndexEntities(List<IndexEntity> indexEntities) {
        this.indexEntities = indexEntities;
    }

    public void addIndexEntities(List<IndexEntity> indexEntities) {
        this.indexEntities.addAll(indexEntities);
    }

    public List<DimensionDesc> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<DimensionDesc> dimensions) {
        this.dimensions = dimensions;
    }

    public List<MeasureDesc> getMeasures() {
        return measures;
    }

    public void setMeasures(List<MeasureDesc> measures) {
        this.measures = measures;
    }
    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public List<DataSegment> getSegments() {
        return segments;
    }
}
