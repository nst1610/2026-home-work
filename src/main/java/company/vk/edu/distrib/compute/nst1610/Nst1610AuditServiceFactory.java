package company.vk.edu.distrib.compute.nst1610;

import company.vk.edu.distrib.compute.AuditService;
import company.vk.edu.distrib.compute.AuditServiceFactory;
import company.vk.edu.distrib.compute.nst1610.audit.Nst1610AuditService;
import java.io.IOException;

public class Nst1610AuditServiceFactory extends AuditServiceFactory {
    @Override
    protected AuditService doCreate(String bootstrapServers, String consumerGroupId) throws IOException {
        return new Nst1610AuditService(bootstrapServers, consumerGroupId);
    }
}
