package its;

import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;

/**
 * Simple logging service.
 *
 * Use this for logging.
 */
@ThriftService
public interface SimpleLogger
{
    /**
     * Log a message
     *
     * @param message the string to log
     */
    @ThriftMethod
    Response log(String message);
}
