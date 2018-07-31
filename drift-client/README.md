# Drift Client

Drift Client is a simple library for creating Thrift clients using annotations.

## Implementing a Client

In Drift, a Thrift client is simply a Java interface annotated with `@ThriftService`. In
addition to annotating the interface directly, Drift supports annotations on super interfaces.
For example, the following describes a Scribe client:

```java
@ThriftService
public interface Scribe
{
    @ThriftMethod
    ResultCode log(List<LogEntry> messages);
}
```

To make the client method asynchronous, simply change the return type of the
corresponding method to a `ListenableFuture`, as shown here:

```java
@ThriftService
public interface Scribe
{
    @ThriftMethod()
    ListenableFuture<List<Integer>> getValues();
}
```

The future will be completed when the server returns a value. When adding listeners
that do non-trivial work when the future is completed, keep in mind that if you do not
provide an executor for listeners to run on, they will run on the NIO threads, and
therefore can potentially block other clients.

### Parameters

In Thrift, method parameters are encoded as a Thrift struct, so each parameter
must have a name and an ID. If you do not specify an ID, Drift will assume the
parameters are numbered starting with `1`. If you do not specify a name, Drift
will attempt to determine the names automatically. For this to work, the code
must be compiled with parameter names enabled (pass the `-parameters` option to `javac`).
If you want to use a different ID or name, simply annotate the parameter as follows:

 ```java
@ThriftService
public interface Scribe
{
    @ThriftMethod
    ResultCode log(@ThriftField(value = 3, name = "mesg") List<LogEntry> messages);
}
```

### Exceptions

As with method parameters, Thrift encodes the response as a struct with field
zero being a standard return and exceptions be stored in higher number fields.
If the Java method throws only one exception annotated with `@ThriftStruct`,
Drift will assume the result struct field id is `1`. Otherwise, you will need to
add the `@ThriftId` annotation to the exception declaration:

```java
@ThriftMethod
void doSomething() throws @ThriftId(1) MyException, @ThriftId(2) MyOther;
```

For asynchronous methods, which do not directly throw exceptions, you will need
to use the ``@ThriftException`` annotation:

```java
@ThriftMethod(exception = {
      @ThriftException(type = MyException.class, id = 1),
      @ThriftException(type = MyOther.class, id = 2),
})
ListenableFuture<Void> doSomething();
```

## Using a Client

A Drift client is thread safe and concurrent, so it can be used by multiple threads
at the same time.  The user of the client simply calls methods on the interface and Drift
manages address selection and connection pooling.  For example:

```java
// create client factory (only create this expensive object once)
DriftClientFactory clientFactory = // see below

// create a client (also only create this once)
Scribe scribe = clientFactory.createDriftClient(Scribe.class);

// use client
scribe.log(ImmutableList.of(new LogEntry("category", "message")));
```

A Drift client can either be created manually using a static factory or injected using Guice.

## Static Drift Client Factory

The following code manually constructs a `DriftClientFactory` using the Netty transport:

```java
// server address
List<HostAndPort> addresses = ImmutableList.of(HostAndPort.fromParts("localhost", 1234));

// expensive services that should only be created once
ThriftCodecManager codecManager = new ThriftCodecManager();
AddressSelector addressSelector = new SimpleAddressSelector(addresses);
DriftNettyClientConfig config = new DriftNettyClientConfig();

// methodInvokerFactory must be closed
DriftNettyMethodInvokerFactory<?> methodInvokerFactory = DriftNettyMethodInvokerFactory
        .createStaticDriftNettyMethodInvokerFactory(config);

// client factory
DriftClientFactory clientFactory = new DriftClientFactory(codecManager, methodInvokerFactory, addressSelector);
```

As you can see, the construction of a `DriftClientFactory` requires a few supporting
services, which are described below.

### ThriftCodecManager

A `ThriftCodecManager` caches the description of every Thrift type used by the clients.  Extracting
service metadata is expensive, so a single `ThriftCodecManager` should be used to create
all the clients you will need.

### AddressSelector

An `AddressSelector` selects host addresses for the client to connect to. Typically,
this service tracks all hosts running the service and selects a random subset to try
for an  invocation. The `AddressSelector` is also notified of addresses that failed to
connect, allowing it to perform simple tracking of host state.

### MethodInvokerFactory

A `MethodInvokerFactory` handles connection and worker thread pools. Connections and
threads are expensive resources, so a single `MethodInvokerFactory` should be used to create
all the clients you will need.  Since the `MethodInvokerFactory` is maintaining pools, a `close`
method is typically provided to shutdown the pools.

There are currently two transport implementations of `MethodInvokerFactory`.  Drift Netty, which
is used in the example above, provides `DriftNettyMethodInvokerFactory`, and Apache Thrift provides
`ApacheThriftMethodInvokerFactory`.  Each transport requires sightly different configuration,
so each transport provides a different configuration class.

## Guice Support

Drift includes optional support for binding clients into Guice.

To bind a client, add a transport implementation module (e.g., `DriftNettyClientModule` or
`ApacheThriftClientModule`), and bind the clients with the fluent `DriftClientBinder`.
The following binds a `Scribe` client that will connect to `example.com:1234` by default:

```java
// server address
List<HostAndPort> addresses = ImmutableList.of(HostAndPort.fromParts("localhost", 1234));

// see io.airlift.bootstrap.Bootstrap for a simpler system to create Guice services with configuration
Injector injector = Guice.createInjector(Stage.PRODUCTION,
        new ConfigurationModule(new ConfigurationFactory(ImmutableMap.of())),
        new DriftNettyClientModule(),
        binder -> driftClientBinder(binder).bindDriftClient(Scribe.class)
                .withAddressSelector(simpleAddressSelector(addresses)));
```

Then, Guice can inject a Thrift client implementation:

```java
@Inject
public MyClass(Scribe scribeClient)
{
    scribeClient.log(entries);
}
```
