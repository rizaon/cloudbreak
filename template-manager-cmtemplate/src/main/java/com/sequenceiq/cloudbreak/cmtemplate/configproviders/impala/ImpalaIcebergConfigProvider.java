package com.sequenceiq.cloudbreak.cmtemplate.configproviders.impala;

import static com.sequenceiq.cloudbreak.cmtemplate.configproviders.ConfigUtils.config;
import static java.util.Optional.ofNullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Component;

import com.cloudera.api.swagger.model.ApiClusterTemplateConfig;
import com.cloudera.api.swagger.model.ApiClusterTemplateRoleConfigGroup;
import com.cloudera.api.swagger.model.ApiClusterTemplateService;
import com.sequenceiq.cloudbreak.cmtemplate.CMRepositoryVersionUtil;
import com.sequenceiq.cloudbreak.cmtemplate.CmTemplateComponentConfigProvider;
import com.sequenceiq.cloudbreak.cmtemplate.CmTemplateProcessor;
import com.sequenceiq.cloudbreak.template.TemplatePreparationObject;

@Component
public class ImpalaIcebergConfigProvider implements CmTemplateComponentConfigProvider {

    private static final String IMPALA_CORE_SITE_SAFETY_VALVE = "impalad_core_site_safety_valve";

    private static final long EXPIRATION_INTERVAL_MS_COORDINATOR = 3600000L;

    private static final long EXPIRATION_INTERVAL_MS_CATALOGSERVER = 604800000L;

    private static final long MAX_CONTENT_LENGTH = 8388608L;

    private static final long MAX_TOTAL_BYTES = 104857600L;

    @Override
    public Map<String, List<ApiClusterTemplateConfig>> getRoleConfigs(CmTemplateProcessor cmTemplate, TemplatePreparationObject source) {
        Optional<ApiClusterTemplateService> service = cmTemplate.getServiceByType(getServiceType());
        if (service.isEmpty()) {
            return Map.of();
        }

        Map<String, List<ApiClusterTemplateConfig>> configs = new HashMap<>();

        for (ApiClusterTemplateRoleConfigGroup rcg : ofNullable(service.get().getRoleConfigGroups()).orElse(new ArrayList<>())) {
            String roleType = rcg.getRoleType();
            if (roleType != null && getRoleTypes().contains(roleType)) {
                configs.put(rcg.getRefName(), getRoleConfigs(rcg));
            }
        }

        return configs;
    }

    private List<ApiClusterTemplateConfig> getRoleConfigs(ApiClusterTemplateRoleConfigGroup rcg) {
        switch (rcg.getRoleType()) {
            case ImpalaRoles.ROLE_IMPALAD:
                boolean isCoordinator = rcg.getConfigs() != null &&
                        rcg.getConfigs().stream().anyMatch(config -> ImpalaRoles.SPECIALIZATION_COORDINATOR_ONLY.equals(config.getValue()));
                if (!isCoordinator) return List.of();
            case ImpalaRoles.ROLE_CATALOGSERVER:
                // rcg is either coordinator or catalogd
                String safetyValveValue = createIcebergManifestCachingProperties(rcg.getRoleType());
                return List.of(
                        config(IMPALA_CORE_SITE_SAFETY_VALVE, safetyValveValue)
                );
            default:
                return List.of();
        }
    }

    private String createIcebergManifestCachingProperties(String roleType) {
        StringBuilder sb = new StringBuilder();
        sb.append("<property><name>iceberg.io-impl</name><value>");
        sb.append("org.apache.iceberg.hadoop.HadoopFileIO");
        sb.append("</value></property>");
        sb.append("<property><name>iceberg.io.manifest.cache-enabled</name><value>");
        sb.append("true");
        sb.append("</value></property>");
        sb.append("<property><name>iceberg.io.manifest.cache.expiration-interval-ms</name><value>");
        if (ImpalaRoles.ROLE_CATALOGSERVER.equals(roleType)) {
            sb.append(EXPIRATION_INTERVAL_MS_CATALOGSERVER);
        } else {
            sb.append(EXPIRATION_INTERVAL_MS_COORDINATOR);
        }
        sb.append("</value></property>");
        sb.append("<property><name>iceberg.io.manifest.cache.max-content-length</name><value>");
        sb.append(MAX_CONTENT_LENGTH);
        sb.append("</value></property>");
        sb.append("<property><name>iceberg.io.manifest.cache.max-total-bytes</name><value>");
        sb.append(MAX_TOTAL_BYTES);
        sb.append("</value></property>");
        return sb.toString();
    }

    @Override
    public String getServiceType() {
        return ImpalaRoles.SERVICE_IMPALA;
    }

    @Override
    public List<String> getRoleTypes() {
        return List.of(ImpalaRoles.ROLE_IMPALAD, ImpalaRoles.ROLE_CATALOGSERVER);
    }

    @Override
    public boolean isConfigurationNeeded(CmTemplateProcessor cmTemplateProcessor, TemplatePreparationObject source) {
        // iceberg manifest caching available since 7.2.16.2.
        String cdhVersion = cmTemplateProcessor.getStackVersion();
        boolean hasIceberg = CMRepositoryVersionUtil.isVersionNewerOrEqualThanLimited(cdhVersion, CMRepositoryVersionUtil.CLOUDERA_STACK_VERSION_7_2_16);
        return hasIceberg && (cmTemplateProcessor.isRoleTypePresentInService(getServiceType(), getRoleTypes())
                || cmTemplateProcessor.isImpalaCoordinatorPresentInService());
    }
}
