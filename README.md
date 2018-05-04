# Drift
[![Maven Central](https://img.shields.io/maven-central/v/io.airlift.drift/drift-root.svg?label=Maven%20Central)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.airlift.drift%22)
[![Build Status](https://travis-ci.org/airlift/drift.svg?branch=master)](https://travis-ci.org/airlift/drift)

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
// create a client
Scribe scribe = clientFactory.createDriftClient(Scribe.class);

// use client
scribe.log(Arrays.asList(new LogEntry("category", "message")));
```

# Detailed Documentation

* [Drift Codec](drift-codec) -- Thrift type annotations and serialization
* [Drift Client](drift-client) -- Thrift client usage
* [Drift Server](drift-server) -- Thrift server usage
