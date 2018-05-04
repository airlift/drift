# Drift Server

Drift Server is a simple library for creating Thrift services using annotations.

## Implementing a Service

In Drift, a Thrift service is simply a Java class annotated with `@ThriftService`. In
addition to annotating the class directly, Drift supports annotations on super classes
or interfaces. A single service instance is used for all incoming client requests, so
it must be thread safe and should handle concurrent requests. For example, the
following describes a Scribe service:

```java
@ThriftService
public class Scribe
{
    @ThriftMethod
    public ResultCode log(List<LogEntry> messages)
    {
        messages.forEach(message -> System.out.println(message.getMessage()));
        return ResultCode.OK;
    }
}
```

To make the service method asynchronous, simply change the return type of the
method to `ListenableFuture`:

```java
@ThriftService
public class Scribe
{
    public ListenableFuture<ResultCode> log(List<LogEntry> messages)
    {
        // process in background using a ListeningExecutorService
        return executor.submit(() -> {
            messages.forEach(message -> System.out.println(message.getMessage()));
            return ResultCode.OK;
        });
    }
}
```

The future should be completed with the value to be returned to the client.
You can use `Futures.catching()` or `Futures.catchingAsync()` to transform
errors into a more appropriate response for the client.

### Parameters and Exceptions

Parameters and exceptions have the same rules as for Drift Client. See
[Parameters](../drift-client/#parameters) and
[Exceptions](../drift-client/#exceptions) for details.

## Creating a Server

The following code manually constructs a `DriftServer` using the Netty transport:

```java
// service handler (must be thread safe)
Scribe service = new Scribe();

// create the server
DriftServer driftServer = new DriftServer(
        new DriftNettyServerTransportFactory(config),
        new ThriftCodecManager(),
        new NullMethodInvocationStatsFactory(),
        ImmutableSet.of(service),
        ImmutableSet.of());

// start the server (it should be shutdown when no longer needed)
driftServer.start();
```

## Guice Support

Drift includes optional support for binding a Thrift server and services into Guice.

To bind a server, add transport implementation module `DriftNettyServerModule` and bind
the clients with the fluent `DriftServerBinder`. The following binds a `Scribe` service:

```java
// see io.airlift.bootstrap.Bootstrap for a simpler system to create Guice services with configuration
Injector injector = Guice.createInjector(Stage.PRODUCTION,
        new ConfigurationModule(new ConfigurationFactory(ImmutableMap.of())),
        new DriftNettyServerModule(),
        binder -> driftServerBinder(binder).bindService(EchoServiceHandler.class));

// Start the server. It should be shutdown when no longer needed.
// Note that startup and shutdown are automatic if using Airlift Bootstrap.
injector.getInstance(DriftServer.class).start();
```

## Multiple Services

Drift allows binding multiple services into a single Thrift server. However,
because the Thrift protocol does not provide any sort of namespacing, all
Thrift method names must be unique across all services.
