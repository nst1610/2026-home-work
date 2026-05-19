package company.vk.edu.distrib.compute.nst1610.audit;

import company.vk.edu.distrib.compute.AuditEvent;

public final class AuditEventUtils {
    private static final String DELIMITER = "\t";
    private static final int EXPECTED_PARTS = 3;

    private AuditEventUtils() {
    }

    public static String encode(AuditEvent event) {
        return event.method() + DELIMITER + event.id() + DELIMITER + event.timestamp();
    }

    public static AuditEvent decode(String serializedEvent) {
        String[] parts = serializedEvent.split(DELIMITER, -1);
        if (parts.length != EXPECTED_PARTS) {
            throw new IllegalArgumentException("Invalid audit event payload");
        }
        return new AuditEvent(parts[0], parts[1], Long.parseLong(parts[2]));
    }
}
