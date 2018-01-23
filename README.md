# Drift

Drift is an easy-to-use, annotation-based Java library for creating Thrift
clients and serializable types.  The client library is similar to JAX-RS 
(HTTP Rest) and the serialization library is similar to JaxB (XML) and Jackson
(JSON), but for Thrift.

## Example 

The following interface defines a client for a Scribe server:

```java
@ThriftService
public interface Scribe
{
    @ThriftMethod
    ResultCode log(List<LogEntry> messages);
}
```

The `log` method above uses the `LogEntry` Thrift struct which is defined as follows:   
 
```java
@ThriftStruct
public class LogEntry
{
    private final String category;
    private final String message;

    @ThriftConstructor
    public LogEntry(String category, String message)
    {
        this.category = category;
        this.message = message;
    }

    @ThriftField(1)
    public String getCategory()
    {
        return category;
    }

    @ThriftField(2)
    public String getMessage()
    {
        return message;
    }
}
```

An instance of the Scribe client can be created using a `DriftClientFactory`:
```java
// expensive services that should only be created once
ThriftCodecManager codecManager = new ThriftCodecManager();
AddressSelector addressSelector = new SimpleAddressSelector(scribeHostAddreses);
DriftNettyClientConfig config = new DriftNettyClientConfig();
// methodInvokerFactory must be closed 
DriftNettyMethodInvokerFactory<?> methodInvokerFactory = DriftNettyMethodInvokerFactory.createStaticDriftNettyMethodInvokerFactory(config);
DriftClientFactory clientFactory = new DriftClientFactory(codecManager, methodInvokerFactory, addressSelector);

// create a client (cached also)
Scribe scribe = clientFactory.createDriftClient(Scribe.class);

// use client
scribe.log(Arrays.asList(new LogEntry("category", "message")));
```

See [Drift Codec](drift-codec) for more information on annotating Thrift types,
and [Drift Client](drift-client) for more information on Thrift client usage. 
