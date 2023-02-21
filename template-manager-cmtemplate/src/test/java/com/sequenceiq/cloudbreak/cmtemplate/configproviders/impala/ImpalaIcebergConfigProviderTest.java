package com.sequenceiq.cloudbreak.cmtemplate.configproviders.impala;

import static com.sequenceiq.cloudbreak.template.TemplatePreparationObject.Builder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import com.cloudera.api.swagger.model.ApiClusterTemplateConfig;
import com.sequenceiq.cloudbreak.cmtemplate.CmTemplateProcessor;
import com.sequenceiq.cloudbreak.domain.StorageLocation;
import com.sequenceiq.cloudbreak.template.TemplatePreparationObject;
import com.sequenceiq.cloudbreak.template.filesystem.StorageLocationView;
import com.sequenceiq.cloudbreak.template.filesystem.s3.S3FileSystemConfigurationsView;
import com.sequenceiq.cloudbreak.template.views.HostgroupView;
import com.sequenceiq.cloudbreak.util.FileReaderUtils;
import com.sequenceiq.common.api.filesystem.S3FileSystem;
import com.sequenceiq.common.api.type.InstanceGroupType;

@ExtendWith(MockitoExtension.class)
class ImpalaIcebergConfigProviderTest {

    private final ImpalaIcebergConfigProvider underTest = new ImpalaIcebergConfigProvider();

    @Test
    public void testImpalaIcebergConfigProvider() {
        CmTemplateProcessor templateProcessor = new CmTemplateProcessor(getBlueprintText("input/clouderamanager-host-with-uppercase.bp"));
        TemplatePreparationObject templatePreparationObject = getTemplatePreparationObject(true);

        Map<String, List<ApiClusterTemplateConfig>> roleConfigs = underTest.getRoleConfigs(templateProcessor, templatePreparationObject);

        String expectedCatalogd = "<property><name>iceberg.io-impl</name><value>org.apache.iceberg.hadoop.HadoopFileIO</value></property>"
                + "<property><name>iceberg.io.manifest.cache-enabled</name><value>true</value></property>"
                + "<property><name>iceberg.io.manifest.cache.expiration-interval-ms</name><value>604800000</value></property>"
                + "<property><name>iceberg.io.manifest.cache.max-content-length</name><value>8388608</value></property>"
                + "<property><name>iceberg.io.manifest.cache.max-total-bytes</name><value>104857600</value></property>";
        List<ApiClusterTemplateConfig> catalogd = roleConfigs.get("impala-CATALOGSERVER-BASE");

        assertEquals(1, catalogd.size());
        assertEquals("impalad_core_site_safety_valve", catalogd.get(0).getName());
        assertEquals(expectedCatalogd, catalogd.get(0).getValue());

        String expectedCoordinator = "<property><name>iceberg.io-impl</name><value>org.apache.iceberg.hadoop.HadoopFileIO</value></property>"
                + "<property><name>iceberg.io.manifest.cache-enabled</name><value>true</value></property>"
                + "<property><name>iceberg.io.manifest.cache.expiration-interval-ms</name><value>3600000</value></property>"
                + "<property><name>iceberg.io.manifest.cache.max-content-length</name><value>8388608</value></property>"
                + "<property><name>iceberg.io.manifest.cache.max-total-bytes</name><value>104857600</value></property>";
        List<ApiClusterTemplateConfig> coordinator = roleConfigs.get("impala-IMPALAD-COORDINATOR");

        assertEquals(1, coordinator.size());
        assertEquals("impalad_core_site_safety_valve", coordinator.get(0).getName());
        assertEquals(expectedCoordinator, coordinator.get(0).getValue());


        List<ApiClusterTemplateConfig> executor = roleConfigs.get("impala-IMPALAD-EXECUTOR");

        assertEquals(0, executor.size());
    }

    @Test
    public void testImpalaIcebergConfigProviderWithLowerCdhVersion() {
        CmTemplateProcessor templateProcessor = new CmTemplateProcessor(getBlueprintText("input/cdp-data-mart.bp"));
        TemplatePreparationObject templatePreparationObject = getTemplatePreparationObject(true);

        assertFalse(underTest.isConfigurationNeeded(templateProcessor, templatePreparationObject));
    }

    private TemplatePreparationObject getTemplatePreparationObject(boolean includeLocations) {
        HostgroupView master = new HostgroupView("master", 1, InstanceGroupType.GATEWAY, 1);
        HostgroupView coordinator = new HostgroupView("coordinator", 1, InstanceGroupType.CORE, 1);
        HostgroupView executor = new HostgroupView("executor", 2, InstanceGroupType.CORE, 2);

        List<StorageLocationView> locations = new ArrayList<>();

        if (includeLocations) {
            locations.add(new StorageLocationView(getHiveWarehouseStorageLocation()));
            locations.add(new StorageLocationView(getHiveWarehouseExternalStorageLocation()));
        }
        S3FileSystemConfigurationsView fileSystemConfigurationsView =
                new S3FileSystemConfigurationsView(new S3FileSystem(), locations, false);
        return Builder.builder()
                .withFileSystemConfigurationView(fileSystemConfigurationsView)
                .withHostgroupViews(Set.of(master, coordinator, executor)).build();
    }

    private StorageLocation getHiveWarehouseStorageLocation() {
        StorageLocation managed = new StorageLocation();
        managed.setProperty("hive.metastore.warehouse.dir");
        managed.setValue("s3a://bucket/warehouse/tablespace/managed/hive");
        return managed;
    }

    private StorageLocation getHiveWarehouseExternalStorageLocation() {
        StorageLocation external = new StorageLocation();
        external.setProperty("hive.metastore.warehouse.external.dir");
        external.setValue("s3a://bucket/warehouse/tablespace/external/hive");
        return external;
    }

    private String getBlueprintText(String path) {
        return FileReaderUtils.readFileFromClasspathQuietly(path);
    }
}