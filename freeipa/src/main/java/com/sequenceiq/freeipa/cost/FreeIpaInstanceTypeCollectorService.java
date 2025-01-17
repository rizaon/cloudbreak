package com.sequenceiq.freeipa.cost;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.inject.Inject;

import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sequenceiq.cloudbreak.auth.ThreadBasedUserCrnProvider;
import com.sequenceiq.cloudbreak.cloud.PricingCache;
import com.sequenceiq.cloudbreak.cloud.model.CloudCredential;
import com.sequenceiq.cloudbreak.cloud.model.ExtendedCloudCredential;
import com.sequenceiq.cloudbreak.common.mappable.CloudPlatform;
import com.sequenceiq.cloudbreak.cost.cloudera.ClouderaCostCache;
import com.sequenceiq.cloudbreak.cost.model.ClusterCostDto;
import com.sequenceiq.cloudbreak.cost.model.DiskCostDto;
import com.sequenceiq.cloudbreak.cost.model.InstanceGroupCostDto;
import com.sequenceiq.freeipa.dto.Credential;
import com.sequenceiq.freeipa.entity.InstanceGroup;
import com.sequenceiq.freeipa.entity.Stack;
import com.sequenceiq.freeipa.entity.Template;
import com.sequenceiq.freeipa.service.CredentialService;

@Service
public class FreeIpaInstanceTypeCollectorService {

    private static final String DEFAULT_ROOT_DISK_TYPE = "standard";

    private static final int DEFAULT_ROOT_DISK_SIZE = 100;

    @Inject
    private CredentialService credentialService;

    @Inject
    private Map<CloudPlatform, PricingCache> pricingCacheMap;

    @Inject
    private ClouderaCostCache clouderaCostCache;

    public ClusterCostDto getAllInstanceTypes(Stack stack) {
        String region = stack.getRegion();
        CloudPlatform cloudPlatform = CloudPlatform.valueOf(stack.getCloudPlatform());
        Credential credential = credentialService.getCredentialByEnvCrn(stack.getEnvironmentCrn());

        ClusterCostDto clusterCostDto = new ClusterCostDto();
        clusterCostDto.setStatus(stack.getStackStatus().getStatus().name());
        clusterCostDto.setRegion(region);

        List<InstanceGroupCostDto> instanceGroupCostDtos = new ArrayList<>();
        for (InstanceGroup instanceGroup : stack.getInstanceGroups()) {
            getInstanceGroupCostDto(region, cloudPlatform, credential, instanceGroup).ifPresent(instanceGroupCostDtos::add);
        }
        clusterCostDto.setInstanceGroups(instanceGroupCostDtos);
        return clusterCostDto;
    }

    private Optional<InstanceGroupCostDto> getInstanceGroupCostDto(String region, CloudPlatform cloudPlatform,
            Credential credential, InstanceGroup instanceGroup) {
        if (pricingCacheMap.containsKey(cloudPlatform)) {
            PricingCache pricingCache = pricingCacheMap.get(cloudPlatform);
            String instanceType = instanceGroup.getTemplate().getInstanceType();
            InstanceGroupCostDto instanceGroupCostDto = new InstanceGroupCostDto();
            instanceGroupCostDto.setPricePerInstance(pricingCache.getPriceForInstanceType(region, instanceType));
            instanceGroupCostDto.setCoresPerInstance(pricingCache.getCpuCountForInstanceType(region, instanceType,
                    convertCredentialToExtendedCloudCredential(credential, cloudPlatform)));
            instanceGroupCostDto.setMemoryPerInstance(pricingCache.getMemoryForInstanceType(region, instanceType,
                    convertCredentialToExtendedCloudCredential(credential, cloudPlatform)));
            instanceGroupCostDto.setClouderaPricePerInstance(clouderaCostCache.getPriceByType(instanceType));
            instanceGroupCostDto.setType(instanceType);
            instanceGroupCostDto.setCount(instanceGroup.getInstanceMetaData().size());

            Template template = instanceGroup.getTemplate();
            List<DiskCostDto> diskCostDtos = new ArrayList<>();
            DiskCostDto rootDiskCostDto = new DiskCostDto(1, template.getRootVolumeSize(),
                    pricingCache.getStoragePricePerGBHour(region, DEFAULT_ROOT_DISK_TYPE, DEFAULT_ROOT_DISK_SIZE));
            diskCostDtos.add(rootDiskCostDto);

            if (template.getVolumeCount() > 0) {
                DiskCostDto diskCostDto = new DiskCostDto(template.getVolumeCount(), template.getVolumeSize(),
                        pricingCache.getStoragePricePerGBHour(region, template.getVolumeType(), template.getVolumeSize()));
                diskCostDtos.add(diskCostDto);
            }

            instanceGroupCostDto.setDisksPerInstance(diskCostDtos);
            return Optional.of(instanceGroupCostDto);
        }
        return Optional.empty();
    }

    private ExtendedCloudCredential convertCredentialToExtendedCloudCredential(Credential credential, CloudPlatform cloudPlatform) {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> credentialAttributes;
        try {
            credentialAttributes = objectMapper.readValue(credential.getAttributes(), new TypeReference<>() { });
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        String userCrn = ThreadBasedUserCrnProvider.getUserCrn();
        String accountId = ThreadBasedUserCrnProvider.getAccountId();
        CloudCredential cloudCredential = new CloudCredential(credential.getCrn(), credential.getName(), credential.getAccountId());
        String cloudPlatformString = cloudPlatform.name();
        Map<String, Object> azureCredMap = (Map<String, Object>) credentialAttributes.get(cloudPlatformString.toLowerCase());
        cloudCredential.putParameter(cloudPlatformString.toLowerCase(), azureCredMap);
        return new ExtendedCloudCredential(cloudCredential, cloudPlatformString, "", userCrn, accountId, List.of());
    }
}
