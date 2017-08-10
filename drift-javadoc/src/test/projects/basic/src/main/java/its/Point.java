package its;

import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.annotations.ThriftStruct;

/**
 * Two dimensional point.
 */
@ThriftStruct
public class Point
{
    @ThriftField(1)
    public int x;

    @ThriftField(2)
    public int y;
}
