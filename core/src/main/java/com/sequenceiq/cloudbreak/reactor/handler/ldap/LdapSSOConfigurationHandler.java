package com.sequenceiq.cloudbreak.reactor.handler.ldap;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.sequenceiq.cloudbreak.auth.altus.VirtualGroupRequest;
import com.sequenceiq.cloudbreak.common.event.Selectable;
import com.sequenceiq.cloudbreak.domain.stack.Stack;
import com.sequenceiq.cloudbreak.dto.LdapView;
import com.sequenceiq.cloudbreak.eventbus.Event;
import com.sequenceiq.cloudbreak.eventbus.EventBus;
import com.sequenceiq.cloudbreak.ldap.LdapConfigService;
import com.sequenceiq.cloudbreak.orchestrator.model.GatewayConfig;
import com.sequenceiq.cloudbreak.reactor.api.event.ldap.LdapSSOConfigurationFailed;
import com.sequenceiq.cloudbreak.reactor.api.event.ldap.LdapSSOConfigurationRequest;
import com.sequenceiq.cloudbreak.reactor.api.event.ldap.LdapSSOConfigurationSuccess;
import com.sequenceiq.cloudbreak.service.GatewayConfigService;
import com.sequenceiq.cloudbreak.service.cluster.ClusterApiConnectors;
import com.sequenceiq.cloudbreak.service.environment.EnvironmentConfigProvider;
import com.sequenceiq.cloudbreak.service.stack.StackService;
import com.sequenceiq.flow.event.EventSelectorUtil;
import com.sequenceiq.flow.reactor.api.handler.EventHandler;

@Component
public class LdapSSOConfigurationHandler implements EventHandler<LdapSSOConfigurationRequest> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LdapSSOConfigurationHandler.class);

    @Inject
    private StackService stackService;

    @Inject
    private EventBus eventBus;

    @Inject
    private ClusterApiConnectors clusterApiConnectors;

    @Inject
    private GatewayConfigService gatewayConfigService;

    @Inject
    private LdapConfigService ldapConfigService;

    @Inject
    private EnvironmentConfigProvider environmentConfigProvider;

    @Override
    public String selector() {
        return EventSelectorUtil.selector(LdapSSOConfigurationRequest.class);
    }

    @Override
    public void accept(Event<LdapSSOConfigurationRequest> ldapConfigurationRequestEvent) {
        Long stackId = ldapConfigurationRequestEvent.getData().getResourceId();
        Selectable response;
        try {
            Stack stack = stackService.getByIdWithListsInTransaction(stackId);
            GatewayConfig primaryGatewayConfig = gatewayConfigService.getPrimaryGatewayConfig(stack);
            LdapView ldapView = ldapConfigService.get(stack.getEnvironmentCrn(), stack.getName()).orElse(null);
            String environmentCrnForVirtualGroups = environmentConfigProvider.getParentEnvironmentCrn(stack.getEnvironmentCrn());
            VirtualGroupRequest virtualGroupRequest = new VirtualGroupRequest(environmentCrnForVirtualGroups, ldapView != null ? ldapView.getAdminGroup() : "");
            clusterApiConnectors.getConnector(stack).clusterSecurityService().setupLdapAndSSO(primaryGatewayConfig.getPublicAddress(), ldapView,
                    virtualGroupRequest);
            response = new LdapSSOConfigurationSuccess(stackId);
        } catch (Exception e) {
            LOGGER.info("Error during LDAP configuration, stackId: " + stackId, e);
            response = new LdapSSOConfigurationFailed(stackId, e);
        }
        eventBus.notify(response.selector(), new Event<>(ldapConfigurationRequestEvent.getHeaders(), response));
    }
}
