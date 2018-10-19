/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.drift.codec.internal.compiler;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.DynamicClassLoader;
import io.airlift.bytecode.FieldDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.ParameterizedType;
import io.airlift.bytecode.Variable;
import io.airlift.bytecode.control.IfStatement;
import io.airlift.bytecode.control.SwitchStatement.SwitchBuilder;
import io.airlift.bytecode.control.WhileLoop;
import io.airlift.bytecode.expression.BytecodeExpression;
import io.airlift.bytecode.instruction.LabelNode;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.ThriftProtocolType;
import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.metadata.DefaultThriftTypeReference;
import io.airlift.drift.codec.metadata.ReflectionHelper;
import io.airlift.drift.codec.metadata.ThriftConstructorInjection;
import io.airlift.drift.codec.metadata.ThriftExtraction;
import io.airlift.drift.codec.metadata.ThriftFieldExtractor;
import io.airlift.drift.codec.metadata.ThriftFieldInjection;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftInjection;
import io.airlift.drift.codec.metadata.ThriftMethodExtractor;
import io.airlift.drift.codec.metadata.ThriftMethodInjection;
import io.airlift.drift.codec.metadata.ThriftParameterInjection;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.codec.metadata.ThriftTypeReference;
import io.airlift.drift.protocol.TProtocolReader;
import io.airlift.drift.protocol.TProtocolWriter;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.bytecode.Access.BRIDGE;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PRIVATE;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.SUPER;
import static io.airlift.bytecode.Access.SYNTHETIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.BytecodeUtils.dumpBytecodeTree;
import static io.airlift.bytecode.ClassGenerator.classGenerator;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.ParameterizedType.typeFromPathName;
import static io.airlift.bytecode.control.SwitchStatement.switchBuilder;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantString;
import static io.airlift.bytecode.expression.BytecodeExpressions.defaultValue;
import static io.airlift.bytecode.expression.BytecodeExpressions.inlineIf;
import static io.airlift.bytecode.expression.BytecodeExpressions.invokeStatic;
import static io.airlift.bytecode.expression.BytecodeExpressions.isNotNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.isNull;
import static io.airlift.bytecode.expression.BytecodeExpressions.newArray;
import static io.airlift.bytecode.expression.BytecodeExpressions.newInstance;
import static io.airlift.drift.codec.ThriftProtocolType.BINARY;
import static io.airlift.drift.codec.ThriftProtocolType.BOOL;
import static io.airlift.drift.codec.ThriftProtocolType.BYTE;
import static io.airlift.drift.codec.ThriftProtocolType.DOUBLE;
import static io.airlift.drift.codec.ThriftProtocolType.ENUM;
import static io.airlift.drift.codec.ThriftProtocolType.I16;
import static io.airlift.drift.codec.ThriftProtocolType.I32;
import static io.airlift.drift.codec.ThriftProtocolType.I64;
import static io.airlift.drift.codec.ThriftProtocolType.LIST;
import static io.airlift.drift.codec.ThriftProtocolType.MAP;
import static io.airlift.drift.codec.ThriftProtocolType.SET;
import static io.airlift.drift.codec.ThriftProtocolType.STRING;
import static io.airlift.drift.codec.ThriftProtocolType.STRUCT;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_UNION_ID;
import static java.lang.String.format;

@NotThreadSafe
public class ThriftCodecByteCodeGenerator<T>
{
    private static final String PACKAGE = "$drift";

    private static final Map<ThriftProtocolType, Method> READ_METHODS;
    private static final Map<ThriftProtocolType, Method> WRITE_METHODS;

    private static final Map<Type, Method> ARRAY_READ_METHODS;
    private static final Map<Type, Method> ARRAY_WRITE_METHODS;

    private static final Method OPTIONAL_READ_METHOD;
    private static final Method OPTIONAL_WRITE_METHOD;

    private final ThriftCodecManager codecManager;
    private final ThriftStructMetadata metadata;
    private final ParameterizedType structType;

    private final ClassDefinition classDefinition;

    private final ConstructorParameters parameters = new ConstructorParameters();

    private final FieldDefinition typeField;
    private final Map<Short, FieldDefinition> codecFields;

    private final ThriftCodec<T> thriftCodec;

    @SuppressWarnings("unchecked")
    @SuppressFBWarnings("DM_DEFAULT_ENCODING")
    public ThriftCodecByteCodeGenerator(
            ThriftCodecManager codecManager,
            ThriftStructMetadata metadata,
            DynamicClassLoader classLoader,
            boolean debug)
    {
        this.codecManager = codecManager;
        this.metadata = metadata;

        structType = type(metadata.getStructClass());

        classDefinition = new ClassDefinition(
                a(PUBLIC, SUPER),
                toCodecType(metadata).getClassName(),
                type(Object.class),
                type(ThriftCodec.class, structType));

        // declare the class fields
        typeField = declareTypeField();
        codecFields = declareCodecFields();

        // declare methods
        defineConstructor();
        defineGetTypeMethod();

        switch (metadata.getMetadataType()) {
            case STRUCT:
                defineReadStructMethod();
                defineWriteStructMethod();
                break;
            case UNION:
                defineReadUnionMethod();
                defineWriteUnionMethod();
                break;
            default:
                throw new IllegalStateException(format("encountered type %s", metadata.getMetadataType()));
        }

        // add the non-generic bridge read and write methods
        defineReadBridgeMethod();
        defineWriteBridgeMethod();

        // dump tree for debugging
        if (debug) {
            System.out.println(dumpBytecodeTree(classDefinition));
        }

        // generate the class
        Class<?> codecClass = classGenerator(classLoader)
                .runAsmVerifier(debug)
                .dumpRawBytecode(debug)
                .outputTo(new PrintWriter(System.out))
                .defineClass(classDefinition, Object.class);

        // instantiate the class
        try {
            Class<?>[] types = parameters.getTypes();
            Constructor<?> constructor = codecClass.getConstructor(types);
            thriftCodec = (ThriftCodec<T>) constructor.newInstance(parameters.getValues());
        }
        catch (Exception e) {
            throw new IllegalStateException("Generated class is invalid", e);
        }
    }

    public ThriftCodec<T> getThriftCodec()
    {
        return thriftCodec;
    }

    /**
     * Declares the private ThriftType field type.
     */
    private FieldDefinition declareTypeField()
    {
        FieldDefinition typeField = classDefinition.declareField(a(PRIVATE, FINAL), "type", type(ThriftType.class));

        // add constructor parameter to initialize this field
        parameters.add(typeField, ThriftType.struct(metadata));

        return typeField;
    }

    /**
     * Declares a field for each delegate codec
     *
     * @return a map from field id to the codec for the field
     */
    private Map<Short, FieldDefinition> declareCodecFields()
    {
        Map<Short, FieldDefinition> codecFields = new TreeMap<>();
        for (ThriftFieldMetadata fieldMetadata : metadata.getFields()) {
            if (needsCodec(fieldMetadata)) {
                ThriftCodec<?> codec = codecManager.getCodec(fieldMetadata.getThriftType());
                String fieldName = fieldMetadata.getName() + "Codec";

                FieldDefinition codecField = classDefinition.declareField(a(PRIVATE, FINAL), fieldName, type(codec.getClass()));
                codecFields.put(fieldMetadata.getId(), codecField);

                parameters.add(codecField, codec);
            }
        }
        return codecFields;
    }

    /**
     * Defines the constructor with a parameter for the ThriftType and the delegate codecs. The
     * constructor simply assigns these parameters to the class fields.
     */
    private void defineConstructor()
    {
        // declare constructor with argument names matching the fields
        List<Parameter> arguments = parameters.getFields().stream()
                .map(field -> arg(field.getName(), field.getType()))
                .collect(toImmutableList());

        MethodDefinition constructor = classDefinition.declareConstructor(a(PUBLIC), arguments);

        // invoke super (Object) constructor
        constructor.getBody()
                .comment("super()")
                .append(constructor.getThis())
                .invokeConstructor(Object.class);

        // this.foo = foo;
        for (int i = 0; i < parameters.getFields().size(); i++) {
            FieldDefinition field = parameters.getFields().get(i);
            Parameter argument = arguments.get(i);
            constructor.getBody().append(
                    constructor.getThis().setField(field, argument));
        }

        // return; (implicit)
        constructor.getBody().ret();
    }

    /**
     * Defines the getType method which simply returns the value of the type field.
     */
    private void defineGetTypeMethod()
    {
        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "getType", type(ThriftType.class));
        method.getBody().append(method.getThis()
                .getField(typeField)
                .ret());
    }

    /**
     * Defines the read method for a struct.
     */
    private void defineReadStructMethod()
    {
        Parameter protocol = arg("protocol", TProtocolReader.class);

        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "read", structType, protocol)
                .addException(Exception.class);

        // ProtocolReader reader = new ProtocolReader(protocol);
        Variable reader = method.getScope().declareVariable(ProtocolReader.class, "reader");
        method.getBody().append(reader.set(newInstance(ProtocolReader.class, protocol)));

        // read all of the data in to local variables
        Map<Short, Variable> structData = readFieldValues(method, reader);

        // build the struct
        Variable result = buildStruct(method, structData);

        // return the instance
        method.getBody().append(result.ret());
    }

    /**
     * Defines the code to read all of the data from the protocol into local variables.
     */
    private Map<Short, Variable> readFieldValues(MethodDefinition method, Variable reader)
    {
        // declare and init local variables here
        Map<Short, Variable> structData = new TreeMap<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            Variable variable = method.getScope().declareVariable(
                    toParameterizedType(field.getThriftType()),
                    "f_" + field.getName());
            structData.put(field.getId(), variable);
            method.getBody().append(variable.set(defaultValue(variable.getType())));
        }

        // protocol.readStructBegin();
        method.getBody().append(reader.invoke("readStructBegin", void.class));

        // while (protocol.nextField())
        WhileLoop whileLoop = new WhileLoop()
                .condition(reader.invoke("nextField", boolean.class));

        // switch (protocol.getFieldId())
        SwitchBuilder switchBuilder = switchBuilder()
                .expression(reader.invoke("getFieldId", short.class));

        // cases for field.id
        buildFieldIdSwitch(method, reader, structData, switchBuilder);

        // default case
        switchBuilder.defaultCase(new BytecodeBlock()
                .append(reader.invoke("skipFieldData", void.class)));

        // finish loop
        whileLoop.body(switchBuilder.build());
        method.getBody().append(whileLoop);

        // protocol.readStructEnd();
        method.getBody().append(reader.invoke("readStructEnd", void.class));

        return structData;
    }

    /**
     * Defines the code to build the struct instance using the data in the local variables.
     */
    private Variable buildStruct(MethodDefinition read, Map<Short, Variable> structData)
    {
        // construct the instance and store it in the instance local variable
        Variable instance = constructStructInstance(read, structData);

        // inject fields
        injectStructFields(read, instance, structData);

        // inject methods
        injectStructMethods(read, instance, structData);

        // invoke factory method if present
        instance = invokeFactoryMethod(read, structData, instance);

        return instance;
    }

    /**
     * Defines the code to construct the struct (or builder) instance and stores it in a local
     * variable.
     */
    private Variable constructStructInstance(MethodDefinition method, Map<Short, Variable> structData)
    {
        // constructor parameters
        List<BytecodeExpression> parameters = new ArrayList<>();
        ThriftConstructorInjection constructor = metadata.getConstructorInjection().get();
        for (ThriftParameterInjection parameter : constructor.getParameters()) {
            BytecodeExpression data = structData.get(parameter.getId());

            // if there is a codec, replace null with the default null value from the codec
            FieldDefinition codecField = codecFields.get(parameter.getId());
            if (codecField != null) {
                BytecodeExpression codec = method.getThis().getField(codecField);
                data = inlineIf(
                        isNull(data),
                        codec.invoke("getType", ThriftType.class)
                                .invoke("getNullValue", Object.class)
                                .cast(data.getType()),
                        data);
            }

            parameters.add(data);
        }

        // create the new instance (or builder)
        Variable instance;
        BytecodeExpression value;
        if (metadata.getBuilderClass() == null) {
            instance = method.getScope().declareVariable(structType, "instance");
            value = newInstance(constructor.getConstructor(), parameters);
        }
        else {
            instance = method.getScope().declareVariable(metadata.getBuilderClass(), "builder");
            value = newInstance(metadata.getBuilderClass(), parameters);
        }

        // store in variable
        method.getBody().append(instance.set(value));
        return instance;
    }

    /**
     * Defines the code to inject data into the struct public fields.
     */
    private void injectStructFields(MethodDefinition method, Variable instance, Map<Short, Variable> structData)
    {
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            method.getBody().append(injectField(field, instance, structData.get(field.getId())));
        }
    }

    /**
     * Defines the code to inject data into the struct methods.
     */
    private void injectStructMethods(MethodDefinition method, Variable instance, Map<Short, Variable> structData)
    {
        for (ThriftMethodInjection methodInjection : metadata.getMethodInjections()) {
            method.getBody().append(injectMethod(methodInjection, instance, structData));
        }
    }

    /**
     * Defines the read method for an union.
     */
    private void defineReadUnionMethod()
    {
        Parameter protocol = arg("protocol", TProtocolReader.class);

        MethodDefinition method = classDefinition.declareMethod(a(PUBLIC), "read", structType, protocol)
                .addException(Exception.class);

        // ProtocolReader reader = new ProtocolReader(protocol);
        Variable reader = method.getScope().declareVariable(type(ProtocolReader.class), "reader");
        method.getBody().append(reader.set(newInstance(ProtocolReader.class, protocol)));

        // field id field.
        Variable fieldId = method.getScope().declareVariable(short.class, "fieldId");
        method.getBody().append(fieldId.set(defaultValue(fieldId.getType())));

        // read all of the data in to local variables
        Map<Short, Variable> unionData = readSingleFieldValue(method, reader, fieldId);

        // build the struct
        Variable result = buildUnion(method, fieldId, unionData);

        // return the instance
        method.getBody().append(result.ret());
    }

    /**
     * Defines the code to read all of the data from the protocol into local variables.
     */
    private Map<Short, Variable> readSingleFieldValue(MethodDefinition method, Variable reader, Variable fieldId)
    {
        // declare and init local variables here
        Map<Short, Variable> unionData = new TreeMap<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            Variable variable = method.getScope().declareVariable(
                    toParameterizedType(field.getThriftType()),
                    "f_" + field.getName());
            unionData.put(field.getId(), variable);
            method.getBody().append(variable.set(defaultValue(variable.getType())));
        }

        // protocol.readStructBegin();
        method.getBody().append(reader.invoke("readStructBegin", void.class));

        // while (protocol.nextField())
        WhileLoop whileLoop = new WhileLoop()
                .condition(reader.invoke("nextField", boolean.class));

        // fieldId = protocol.getFieldId()
        whileLoop.body().append(fieldId.set(reader.invoke("getFieldId", short.class)));

        // switch (fieldId)
        SwitchBuilder switchBuilder = switchBuilder().expression(fieldId);

        // cases for field.id
        buildFieldIdSwitch(method, reader, unionData, switchBuilder);

        // default case
        switchBuilder.defaultCase(new BytecodeBlock()
                .append(reader.invoke("skipFieldData", void.class)));

        // finish loop
        whileLoop.body().append(switchBuilder.build());
        method.getBody().append(whileLoop);

        // protocol.readStructEnd();
        method.getBody().append(reader.invoke("readStructEnd", void.class));

        return unionData;
    }

    /**
     * Defines the code to build the struct instance using the data in the local variables.
     */
    private Variable buildUnion(MethodDefinition method, Variable fieldId, Map<Short, Variable> unionData)
    {
        // construct the instance and store it in the instance local variable
        Variable instance = constructUnionInstance(method, fieldId, unionData);

        // switch (fieldId)
        SwitchBuilder switchBuilder = switchBuilder().expression(fieldId);

        // cases for field.id
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            BytecodeBlock block = injectField(field, instance, unionData.get(field.getId()));

            field.getMethodInjection().ifPresent(injection ->
                    block.append(injectMethod(injection, instance, unionData)));

            switchBuilder.addCase(field.getId(), block);
        }

        // finish switch
        method.getBody().append(switchBuilder.build());

        // find the @ThriftUnionId field
        ThriftFieldMetadata idField = getOnlyElement(metadata.getFields(THRIFT_UNION_ID));

        injectIdField(method, idField, instance, fieldId);

        // invoke factory method if present
        invokeFactoryMethod(method, unionData, instance);

        return instance;
    }

    /**
     * Defines the code to construct the union (or builder) instance and stores it in a local
     * variable.
     */
    private Variable constructUnionInstance(MethodDefinition method, Variable fieldId, Map<Short, Variable> unionData)
    {
        // declare variable for instance (or builder)
        Variable instance;
        if (metadata.getBuilderClass() == null) {
            instance = method.getScope().declareVariable(structType, "instance");
        }
        else {
            instance = method.getScope().declareVariable(metadata.getBuilderClass(), "builder");
        }

        // switch (fieldId)
        SwitchBuilder switchBuilder = switchBuilder()
                .expression(fieldId);

        // cases for field.id
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            field.getConstructorInjection().ifPresent(constructor -> {
                Variable fieldValue = unionData.get(field.getId());
                switchBuilder.addCase(field.getId(), new BytecodeBlock()
                        .append(instance.set(newInstance(constructor.getConstructor(), fieldValue))));
            });
        }

        // use no-args constructor if present
        BytecodeBlock defaultBlock = new BytecodeBlock();
        if (metadata.getConstructorInjection().isPresent()) {
            ThriftConstructorInjection constructor = metadata.getConstructorInjection().get();
            defaultBlock.append(instance.set(newInstance(constructor.getConstructor())));
        }
        else {
            BytecodeExpression exception = newInstance(
                    IllegalStateException.class,
                    invokeStatic(
                            String.class,
                            "format",
                            String.class,
                            constantString("No constructor for union [%s] with field ID [%s] found"),
                            newArray(type(Object[].class),
                                    constantString(metadata.getStructClass().getName()),
                                    fieldId.cast(Object.class))));
            defaultBlock.append(exception).throwObject();
        }

        // finish switch
        method.getBody().append(switchBuilder
                .defaultCase(defaultBlock)
                .build());

        return instance;
    }

    private static BytecodeBlock injectField(ThriftFieldMetadata field, Variable instance, Variable sourceVariable)
    {
        BytecodeBlock block = new BytecodeBlock();
        for (ThriftInjection injection : field.getInjections()) {
            if (injection instanceof ThriftFieldInjection) {
                ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;

                // write field
                BytecodeNode writeField = new BytecodeBlock()
                        .append(instance.setField(fieldInjection.getField(), sourceVariable));

                // if field is an Object && field != null
                if (!fieldInjection.getField().getType().isPrimitive()) {
                    writeField = new IfStatement()
                            .condition(isNotNull(sourceVariable))
                            .ifTrue(writeField);
                }

                block.append(writeField);
            }
        }
        return block;
    }

    private void buildFieldIdSwitch(MethodDefinition method, Variable reader, Map<Short, Variable> structData, SwitchBuilder switchBuilder)
    {
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            // get read method
            Method readMethod = getReadMethod(field.getThriftType());
            if (readMethod == null) {
                throw new IllegalArgumentException("Unsupported field type " + field.getThriftType().getProtocolType());
            }

            // get ThriftTypeCodec for this field
            BytecodeExpression result;
            FieldDefinition codecField = codecFields.get(field.getId());
            if (codecField != null) {
                result = reader.invoke(readMethod, method.getThis().getField(codecField));
            }
            else {
                result = reader.invoke(readMethod);
            }

            // todo this cast should be based on readMethod return type and fieldType (or coercion type)
            // add cast if necessary
            if (needsCastAfterRead(field, readMethod)) {
                result = result.cast(toParameterizedType(field.getThriftType()));
            }

            // coerce the type
            if (field.getCoercion().isPresent()) {
                result = invokeStatic(field.getCoercion().get().getFromThrift(), result);
            }

            // store protocol value
            switchBuilder.addCase(field.getId(), new BytecodeBlock()
                    .append(structData.get(field.getId()).set(result)));
        }
    }

    private static BytecodeBlock injectMethod(ThriftMethodInjection methodInjection, Variable instance, Map<Short, Variable> structData)
    {
        String methodName = methodInjection.getMethod().toGenericString();
        LabelNode invokeLabel = new LabelNode("invoke_" + methodName);
        LabelNode skipInvokeLabel = new LabelNode("skip_invoke_" + methodName);
        BytecodeBlock read = new BytecodeBlock();

        // if any parameter is non-null, invoke the method
        for (ThriftParameterInjection parameter : methodInjection.getParameters()) {
            if (!isParameterTypeJavaPrimitive(parameter)) {
                read.getVariable(structData.get(parameter.getId()))
                        .ifNotNullGoto(invokeLabel);
            }
            else {
                read.gotoLabel(invokeLabel);
            }
        }
        read.gotoLabel(skipInvokeLabel);

        // invoke the method
        read.visitLabel(invokeLabel)
                .getVariable(instance);

        // push parameters on stack
        for (ThriftParameterInjection parameter : methodInjection.getParameters()) {
            read.getVariable(structData.get(parameter.getId()));
        }

        // invoke the method
        read.invokeVirtual(methodInjection.getMethod());

        // if method has a return, we need to pop it off the stack
        if (methodInjection.getMethod().getReturnType() != void.class) {
            read.pop();
        }

        // skip invocation
        read.visitLabel(skipInvokeLabel);

        return read;
    }

    /**
     * Defines the code that calls the builder factory method.
     */
    private Variable invokeFactoryMethod(MethodDefinition method, Map<Short, Variable> structData, Variable instance)
    {
        if (metadata.getBuilderMethod().isPresent()) {
            ThriftMethodInjection builderMethod = metadata.getBuilderMethod().get();

            List<Variable> parameters = builderMethod.getParameters().stream()
                    .map(ThriftParameterInjection::getId)
                    .map(structData::get)
                    .collect(toImmutableList());

            BytecodeExpression result = instance.invoke(builderMethod.getMethod(), parameters);

            instance = method.getScope().declareVariable(structType, "instance");
            method.getBody().append(instance.set(result));
        }

        return instance;
    }

    private static void injectIdField(MethodDefinition method, ThriftFieldMetadata field, Variable instance, Variable fieldId)
    {
        for (ThriftInjection injection : field.getInjections()) {
            if (injection instanceof ThriftFieldInjection) {
                ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;

                // write field
                BytecodeNode writeField = new BytecodeBlock()
                        .append(instance.setField(fieldInjection.getField(), fieldId));

                // if field is an Object && field != null
                if (!fieldInjection.getField().getType().isPrimitive()) {
                    writeField = new IfStatement()
                            .condition(isNotNull(fieldId))
                            .ifTrue(writeField);
                }

                method.getBody().append(writeField);
            }
        }
    }

    /**
     * Define the write method.
     */
    private void defineWriteStructMethod()
    {
        Parameter struct = arg("struct", structType);
        Parameter protocol = arg("protocol", TProtocolWriter.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "write",
                null,
                struct,
                protocol);
        BytecodeBlock body = method.getBody();

        // ProtocolWriter writer = new ProtocolWriter(protocol);
        Variable writer = method.getScope().declareVariable(type(ProtocolWriter.class), "writer");
        body.append(writer.set(newInstance(ProtocolWriter.class, protocol)));

        // writer.writeStructBegin("bonk");
        body.append(writer.invoke("writeStructBegin", void.class, constantString(metadata.getStructName())));

        // write fields
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            body.append(writeField(method, writer, field));
        }

        // writer.writeStructEnd();
        body.append(writer.invoke("writeStructEnd", void.class));

        body.ret();
    }

    /**
     * Define the write method.
     */
    private void defineWriteUnionMethod()
    {
        Parameter struct = arg("struct", structType);
        Parameter protocol = arg("protocol", TProtocolWriter.class);

        MethodDefinition method = classDefinition.declareMethod(
                a(PUBLIC),
                "write",
                null,
                struct,
                protocol);
        BytecodeBlock body = method.getBody();

        // ProtocolWriter writer = new ProtocolWriter(protocol);
        Variable writer = method.getScope().declareVariable(type(ProtocolWriter.class), "writer");
        body.append(writer.set(newInstance(ProtocolWriter.class, protocol)));

        // writer.writeStructBegin("bonk");
        body.append(writer.invoke("writeStructBegin", void.class, constantString(metadata.getStructName())));

        // find the @ThriftUnionId field
        ThriftFieldMetadata idField = getOnlyElement(metadata.getFields(THRIFT_UNION_ID));

        // load its value
        BytecodeExpression value = getFieldValue(method, idField);

        // switch (fieldId)
        SwitchBuilder switchBuilder = switchBuilder().expression(value);

        // write fields
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            switchBuilder.addCase(field.getId(), writeField(method, writer, field));
        }

        // finish switch
        method.getBody().append(switchBuilder.build());

        // writer.writeStructEnd();
        body.append(writer.invoke("writeStructEnd", void.class));

        body.ret();
    }

    private BytecodeBlock writeField(MethodDefinition method, Variable writer, ThriftFieldMetadata field)
    {
        LabelNode fieldIsNull = new LabelNode("field_is_null_" + field.getName());
        LabelNode fieldEnd = new LabelNode("field_end_" + field.getName());
        BytecodeBlock write = new BytecodeBlock();

        // push writer
        write.getVariable(writer);

        // push (String) field.name
        write.push(field.getName());

        // push (short) field.id
        write.push(field.getId());

        // push ThriftTypeCodec for this field
        FieldDefinition codecField = codecFields.get(field.getId());
        if (codecField != null) {
            write.append(method.getThis().getField(codecField));
        }

        // push field value
        write.append(getFieldValue(method, field));

        // if field value is null, don't coerce or write the field
        if (!isFieldTypeJavaPrimitive(field)) {
            // ifNullGoto consumes the top of the stack, so we need to duplicate the value
            write.dup();
            write.ifNullGoto(fieldIsNull);
        }

        // coerce value
        if (field.getCoercion().isPresent()) {
            write.invokeStatic(field.getCoercion().get().getToThrift());

            // if coerced value is null, don't write the field
            if (!isProtocolTypeJavaPrimitive(field)) {
                write.dup();
                write.ifNullGoto(fieldIsNull);
            }
        }

        // write value
        Method writeMethod = getWriteMethod(field.getThriftType());
        if (writeMethod == null) {
            throw new IllegalArgumentException("Unsupported field type " + field.getThriftType().getProtocolType());
        }
        write.invokeVirtual(writeMethod);

        //
        // If not written because of a null, clean-up the stack
        if (!isProtocolTypeJavaPrimitive(field) || !isFieldTypeJavaPrimitive(field)) {
            // value was written so skip cleanup
            write.gotoLabel(fieldEnd);

            // cleanup stack for null field value
            write.visitLabel(fieldIsNull);
            // pop value
            write.pop();
            // pop codec
            if (codecField != null) {
                write.pop();
            }
            // pop id
            write.pop();
            // pop name
            write.pop();
            // pop protocol
            write.pop();

            write.visitLabel(fieldEnd);
        }

        return write;
    }

    private BytecodeExpression getFieldValue(MethodDefinition method, ThriftFieldMetadata field)
    {
        BytecodeExpression value = method.getScope().getVariable("struct");
        if (field.getExtraction().isPresent()) {
            ThriftExtraction extraction = field.getExtraction().get();
            if (extraction instanceof ThriftFieldExtractor) {
                ThriftFieldExtractor fieldExtractor = (ThriftFieldExtractor) extraction;
                value = value.getField(fieldExtractor.getField());
                if (fieldExtractor.isGeneric()) {
                    value = value.cast(type(fieldExtractor.getType()));
                }
            }
            else if (extraction instanceof ThriftMethodExtractor) {
                ThriftMethodExtractor methodExtractor = (ThriftMethodExtractor) extraction;
                value = value.invoke(methodExtractor.getMethod());
                if (methodExtractor.isGeneric()) {
                    value = value.cast(type(methodExtractor.getType()));
                }
            }
        }
        return value;
    }

    /**
     * Defines the generics bridge method with untyped args to the type specific read method.
     */
    private void defineReadBridgeMethod()
    {
        Parameter protocol = arg("protocol", TProtocolReader.class);

        MethodDefinition method = new MethodDefinition(
                classDefinition,
                a(PUBLIC, BRIDGE, SYNTHETIC),
                "read",
                type(Object.class),
                protocol)
                .addException(Exception.class);

        method.getBody().append(method.getThis()
                .invoke("read", structType, ImmutableList.of(protocol))
                .ret());

        classDefinition.addMethod(method);
    }

    /**
     * Defines the generics bridge method with untyped args to the type specific write method.
     */
    private void defineWriteBridgeMethod()
    {
        Parameter struct = arg("struct", Object.class);
        Parameter protocol = arg("protocol", TProtocolWriter.class);

        MethodDefinition method = new MethodDefinition(
                classDefinition,
                a(PUBLIC, BRIDGE, SYNTHETIC),
                "write",
                null,
                struct,
                protocol)
                .addException(Exception.class);

        method.getBody().append(method.getThis()
                .invoke("write",
                        type(void.class),
                        ImmutableList.of(structType, protocol.getType()),
                        ImmutableList.of(struct.cast(structType), protocol))
                .ret());

        classDefinition.addMethod(method);
    }

    private static boolean isParameterTypeJavaPrimitive(ThriftParameterInjection parameter)
    {
        return isJavaPrimitive(TypeToken.of(parameter.getJavaType()));
    }

    private static boolean isFieldTypeJavaPrimitive(ThriftFieldMetadata field)
    {
        return isJavaPrimitive(TypeToken.of(field.getThriftType().getJavaType()));
    }

    private static boolean isProtocolTypeJavaPrimitive(ThriftFieldMetadata field)
    {
        if (field.getThriftType().isCoerced()) {
            return isJavaPrimitive(TypeToken.of(field.getThriftType().getUncoercedType().getJavaType()));
        }
        else {
            return isJavaPrimitive(TypeToken.of(field.getThriftType().getJavaType()));
        }
    }

    private static boolean isJavaPrimitive(TypeToken<?> typeToken)
    {
        return typeToken
                .getRawType()
                .isPrimitive();
    }

    private static boolean needsCastAfterRead(ThriftFieldMetadata field, Method readMethod)
    {
        Class<?> methodReturn = readMethod.getReturnType();
        Class<?> fieldType;
        if (field.getCoercion().isPresent()) {
            fieldType = field.getCoercion().get().getFromThrift().getParameterTypes()[0];
        }
        else {
            fieldType = TypeToken.of(field.getThriftType().getJavaType()).getRawType();
        }
        return !fieldType.isAssignableFrom(methodReturn);
    }

    private static boolean needsCodec(ThriftFieldMetadata fieldMetadata)
    {
        Type javaType = fieldMetadata.getThriftType().getJavaType();
        if (ReflectionHelper.isArray(javaType)) {
            return false;
        }

        if (isOptionalWrapper(javaType)) {
            return true;
        }

        ThriftProtocolType protocolType = fieldMetadata.getThriftType().getProtocolType();
        return protocolType == ENUM ||
                protocolType == STRUCT ||
                protocolType == SET ||
                protocolType == LIST ||
                protocolType == MAP;
    }

    private static ParameterizedType toCodecType(ThriftStructMetadata metadata)
    {
        String className = type(metadata.getStructClass()).getClassName();
        return typeFromPathName(PACKAGE + "/" + className + "Codec");
    }

    private static boolean isOptionalWrapper(Type javaType)
    {
        return ReflectionHelper.isOptional(javaType) ||
                javaType == OptionalDouble.class ||
                javaType == OptionalInt.class ||
                javaType == OptionalLong.class;
    }

    private static class ConstructorParameters
    {
        private final List<FieldDefinition> fields = new ArrayList<>();
        private final List<Object> values = new ArrayList<>();

        private void add(FieldDefinition field, Object value)
        {
            fields.add(field);
            values.add(value);
        }

        public List<FieldDefinition> getFields()
        {
            return fields;
        }

        public Object[] getValues()
        {
            return values.toArray(new Object[0]);
        }

        public Class<?>[] getTypes()
        {
            return values.stream()
                    .map(Object::getClass)
                    .toArray(Class<?>[]::new);
        }
    }

    public static ParameterizedType toParameterizedType(ThriftType type)
    {
        return toParameterizedType(new DefaultThriftTypeReference(type));
    }

    public static ParameterizedType toParameterizedType(ThriftTypeReference typeRef)
    {
        if (ReflectionHelper.isArray(typeRef.getJavaType())) {
            return type((Class<?>) typeRef.getJavaType());
        }

        if (ReflectionHelper.isOptional(typeRef.getJavaType())) {
            return type(Optional.class, toParameterizedType(typeRef.get().getValueTypeReference()));
        }

        switch (typeRef.getProtocolType()) {
            case BOOL:
            case BYTE:
            case DOUBLE:
            case I16:
            case I32:
            case I64:
            case STRING:
            case BINARY:
            case STRUCT:
            case ENUM:
                return type((Class<?>) typeRef.getJavaType());
            case MAP:
                return type(Map.class, toParameterizedType(typeRef.get().getKeyTypeReference()), toParameterizedType(typeRef.get().getValueTypeReference()));
            case SET:
                return type(Set.class, toParameterizedType(typeRef.get().getValueTypeReference()));
            case LIST:
                return type(List.class, toParameterizedType(typeRef.get().getValueTypeReference()));
            default:
                throw new IllegalArgumentException("Unsupported thrift field type " + typeRef.getJavaType());
        }
    }

    private Method getWriteMethod(ThriftType thriftType)
    {
        if (ReflectionHelper.isArray(thriftType.getJavaType())) {
            return ARRAY_WRITE_METHODS.get(thriftType.getJavaType());
        }
        if (isOptionalWrapper(thriftType.getJavaType())) {
            return OPTIONAL_WRITE_METHOD;
        }
        return WRITE_METHODS.get(thriftType.getProtocolType());
    }

    private Method getReadMethod(ThriftType thriftType)
    {
        if (ReflectionHelper.isArray(thriftType.getJavaType())) {
            return ARRAY_READ_METHODS.get(thriftType.getJavaType());
        }
        if (isOptionalWrapper(thriftType.getJavaType())) {
            return OPTIONAL_READ_METHOD;
        }
        return READ_METHODS.get(thriftType.getProtocolType());
    }

    static {
        ImmutableMap.Builder<ThriftProtocolType, Method> writeBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<ThriftProtocolType, Method> readBuilder = ImmutableMap.builder();

        try {
            writeBuilder.put(BOOL, ProtocolWriter.class.getMethod("writeBoolField", String.class, short.class, boolean.class));
            writeBuilder.put(BYTE, ProtocolWriter.class.getMethod("writeByteField", String.class, short.class, byte.class));
            writeBuilder.put(DOUBLE, ProtocolWriter.class.getMethod("writeDoubleField", String.class, short.class, double.class));
            writeBuilder.put(I16, ProtocolWriter.class.getMethod("writeI16Field", String.class, short.class, short.class));
            writeBuilder.put(I32, ProtocolWriter.class.getMethod("writeI32Field", String.class, short.class, int.class));
            writeBuilder.put(I64, ProtocolWriter.class.getMethod("writeI64Field", String.class, short.class, long.class));
            writeBuilder.put(STRING, ProtocolWriter.class.getMethod("writeStringField", String.class, short.class, String.class));
            writeBuilder.put(BINARY, ProtocolWriter.class.getMethod("writeBinaryField", String.class, short.class, ByteBuffer.class));
            writeBuilder.put(STRUCT, ProtocolWriter.class.getMethod("writeStructField", String.class, short.class, ThriftCodec.class, Object.class));
            writeBuilder.put(MAP, ProtocolWriter.class.getMethod("writeMapField", String.class, short.class, ThriftCodec.class, Map.class));
            writeBuilder.put(SET, ProtocolWriter.class.getMethod("writeSetField", String.class, short.class, ThriftCodec.class, Set.class));
            writeBuilder.put(LIST, ProtocolWriter.class.getMethod("writeListField", String.class, short.class, ThriftCodec.class, List.class));
            writeBuilder.put(ENUM, ProtocolWriter.class.getMethod("writeEnumField", String.class, short.class, ThriftCodec.class, Enum.class));

            readBuilder.put(BOOL, ProtocolReader.class.getMethod("readBoolField"));
            readBuilder.put(BYTE, ProtocolReader.class.getMethod("readByteField"));
            readBuilder.put(DOUBLE, ProtocolReader.class.getMethod("readDoubleField"));
            readBuilder.put(I16, ProtocolReader.class.getMethod("readI16Field"));
            readBuilder.put(I32, ProtocolReader.class.getMethod("readI32Field"));
            readBuilder.put(I64, ProtocolReader.class.getMethod("readI64Field"));
            readBuilder.put(STRING, ProtocolReader.class.getMethod("readStringField"));
            readBuilder.put(BINARY, ProtocolReader.class.getMethod("readBinaryField"));
            readBuilder.put(STRUCT, ProtocolReader.class.getMethod("readStructField", ThriftCodec.class));
            readBuilder.put(MAP, ProtocolReader.class.getMethod("readMapField", ThriftCodec.class));
            readBuilder.put(SET, ProtocolReader.class.getMethod("readSetField", ThriftCodec.class));
            readBuilder.put(LIST, ProtocolReader.class.getMethod("readListField", ThriftCodec.class));
            readBuilder.put(ENUM, ProtocolReader.class.getMethod("readEnumField", ThriftCodec.class));
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        WRITE_METHODS = writeBuilder.build();
        READ_METHODS = readBuilder.build();

        ImmutableMap.Builder<Type, Method> arrayWriteBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<Type, Method> arrayReadBuilder = ImmutableMap.builder();

        try {
            arrayWriteBuilder.put(boolean[].class, ProtocolWriter.class.getMethod("writeBoolArrayField", String.class, short.class, boolean[].class));
            arrayWriteBuilder.put(short[].class, ProtocolWriter.class.getMethod("writeI16ArrayField", String.class, short.class, short[].class));
            arrayWriteBuilder.put(int[].class, ProtocolWriter.class.getMethod("writeI32ArrayField", String.class, short.class, int[].class));
            arrayWriteBuilder.put(long[].class, ProtocolWriter.class.getMethod("writeI64ArrayField", String.class, short.class, long[].class));
            arrayWriteBuilder.put(double[].class, ProtocolWriter.class.getMethod("writeDoubleArrayField", String.class, short.class, double[].class));

            arrayReadBuilder.put(boolean[].class, ProtocolReader.class.getMethod("readBoolArrayField"));
            arrayReadBuilder.put(short[].class, ProtocolReader.class.getMethod("readI16ArrayField"));
            arrayReadBuilder.put(int[].class, ProtocolReader.class.getMethod("readI32ArrayField"));
            arrayReadBuilder.put(long[].class, ProtocolReader.class.getMethod("readI64ArrayField"));
            arrayReadBuilder.put(double[].class, ProtocolReader.class.getMethod("readDoubleArrayField"));

            // byte[] is encoded as BINARY which should use the normal rules above, but it
            // simpler to add explicit handling here
            arrayWriteBuilder.put(byte[].class, ProtocolWriter.class.getMethod("writeBinaryField", String.class, short.class, ByteBuffer.class));
            arrayReadBuilder.put(byte[].class, ProtocolReader.class.getMethod("readBinaryField"));
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        ARRAY_WRITE_METHODS = arrayWriteBuilder.build();
        ARRAY_READ_METHODS = arrayReadBuilder.build();

        try {
            OPTIONAL_READ_METHOD = ProtocolReader.class.getMethod("readField", ThriftCodec.class);
            OPTIONAL_WRITE_METHOD = ProtocolWriter.class.getMethod("writeField", String.class, short.class, ThriftCodec.class, Object.class);
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }
}
