package com.sequenceiq.datalake.service.consumption;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.google.common.base.Strings;
import com.sequenceiq.cloudbreak.auth.ThreadBasedUserCrnProvider;
import com.sequenceiq.cloudbreak.auth.altus.EntitlementService;
import com.sequenceiq.consumption.api.v1.consumption.model.common.ResourceType;
import com.sequenceiq.consumption.api.v1.consumption.model.request.StorageConsumptionRequest;
import com.sequenceiq.datalake.entity.SdxCluster;

@Service
public class ConsumptionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumptionService.class);

    @Inject
    private EntitlementService entitlementService;

    @Inject
    private ConsumptionClientService consumptionClientService;

    @Value("${datalake.consumption.enabled:false}")
    private boolean consumptionEnabled;

    public void scheduleStorageConsumptionCollectionIfNeeded(SdxCluster sdxCluster) {
        String accountId = sdxCluster.getAccountId();
        if (consumptionEnabled && entitlementService.isCdpSaasEnabled(accountId)) {
            scheduleStorageConsumptionCollection(sdxCluster);
        } else {
            LOGGER.info("Skipping storage consumption collection scheduling because " +
                    (consumptionEnabled ? String.format("CDP_SAAS entitlement is missing for account '%s'", accountId) : "it is disabled for the deployment"));
        }
    }

    private void scheduleStorageConsumptionCollection(SdxCluster sdxCluster) {
        String storageBaseLocation = sdxCluster.getCloudStorageBaseLocation();
        if (!Strings.isNullOrEmpty(storageBaseLocation)) {
            scheduleStorageConsumptionCollectionForStorageLocation(sdxCluster, storageBaseLocation);
        } else {
            LOGGER.warn("Skipping storage consumption collection scheduling for storage base location because it is not provided: " +
                            "cloudStorageBaseLocation='{}'", storageBaseLocation);
        }
    }

    private void scheduleStorageConsumptionCollectionForStorageLocation(SdxCluster sdxCluster, String storageLocation) {
        String userCrn = ThreadBasedUserCrnProvider.getUserCrn();
        StorageConsumptionRequest request = new StorageConsumptionRequest();
        request.setEnvironmentCrn(sdxCluster.getEnvCrn());
        request.setMonitoredResourceCrn(sdxCluster.getResourceCrn());
        request.setMonitoredResourceName(sdxCluster.getName());
        request.setMonitoredResourceType(ResourceType.DATALAKE);
        request.setStorageLocation(storageLocation);
        String accountId = sdxCluster.getAccountId();
        LOGGER.info("Executing storage consumption collection scheduling for storage base location: account '{}', user '{}' and request '{}'",
                accountId, userCrn, request);
        consumptionClientService.scheduleStorageConsumptionCollection(accountId, request, userCrn);
    }

    public void unscheduleStorageConsumptionCollectionIfNeeded(SdxCluster sdxCluster) {
        String accountId = sdxCluster.getAccountId();
        if (consumptionEnabled && entitlementService.isCdpSaasEnabled(accountId)) {
            unscheduleStorageConsumptionCollection(sdxCluster);
        } else {
            LOGGER.info("Skipping storage consumption collection unscheduling because " +
                    (consumptionEnabled ? String.format("CDP_SAAS entitlement is missing for account '%s'", accountId) : "it is disabled for the deployment"));
        }
    }

    private void unscheduleStorageConsumptionCollection(SdxCluster sdxCluster) {
        String storageBaseLocation = sdxCluster.getCloudStorageBaseLocation();
        String userCrn = ThreadBasedUserCrnProvider.getUserCrn();
        if (!Strings.isNullOrEmpty(storageBaseLocation)) {
            unscheduleStorageConsumptionCollectionForStorageLocation(sdxCluster.getAccountId(), sdxCluster.getResourceCrn(), storageBaseLocation, userCrn);
        } else {
            LOGGER.warn("Skipping storage consumption collection unscheduling for storage base location because it is not provided: " +
                            "cloudStorageBaseLocation='{}'", storageBaseLocation);
        }
    }

    private void unscheduleStorageConsumptionCollectionForStorageLocation(String accountId, String monitoredResourceCrn,
            String storageLocation, String initiatorUserCrn) {
        LOGGER.info("Executing storage consumption collection unscheduling for storage base location: " +
                        "account '{}', user '{}', resource '{}' and storage location '{}'",
                accountId, initiatorUserCrn, monitoredResourceCrn, storageLocation);
        consumptionClientService.unscheduleStorageConsumptionCollection(accountId, monitoredResourceCrn, storageLocation, initiatorUserCrn);
    }

}
