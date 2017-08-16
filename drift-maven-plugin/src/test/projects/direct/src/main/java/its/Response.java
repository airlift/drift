package its;

import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;

@ThriftStruct
public class Response
{
    private int type;

    /**
     * Get response type
     */
    @ThriftField(1)
    public int getType()
    {
        return type;
    }

    /**
     * Set response type
     */
    @ThriftField
    public void setType(int type)
    {
        this.type = type;
    }
}
