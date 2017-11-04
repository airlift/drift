/*
 * Copyright (C) 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.airlift.drift.codec.internal.compiler;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.drift.codec.ThriftCodec;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.ThriftProtocolType;
import io.airlift.drift.codec.internal.ProtocolReader;
import io.airlift.drift.codec.internal.ProtocolWriter;
import io.airlift.drift.codec.internal.compiler.byteCode.CaseStatement;
import io.airlift.drift.codec.internal.compiler.byteCode.ClassDefinition;
import io.airlift.drift.codec.internal.compiler.byteCode.FieldDefinition;
import io.airlift.drift.codec.internal.compiler.byteCode.LocalVariableDefinition;
import io.airlift.drift.codec.internal.compiler.byteCode.MethodDefinition;
import io.airlift.drift.codec.internal.compiler.byteCode.NamedParameterDefinition;
import io.airlift.drift.codec.internal.compiler.byteCode.ParameterizedType;
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
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;

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

import static com.google.common.collect.Iterables.getOnlyElement;
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
import static io.airlift.drift.codec.internal.compiler.byteCode.Access.BRIDGE;
import static io.airlift.drift.codec.internal.compiler.byteCode.Access.FINAL;
import static io.airlift.drift.codec.internal.compiler.byteCode.Access.PRIVATE;
import static io.airlift.drift.codec.internal.compiler.byteCode.Access.PUBLIC;
import static io.airlift.drift.codec.internal.compiler.byteCode.Access.SUPER;
import static io.airlift.drift.codec.internal.compiler.byteCode.Access.SYNTHETIC;
import static io.airlift.drift.codec.internal.compiler.byteCode.Access.a;
import static io.airlift.drift.codec.internal.compiler.byteCode.CaseStatement.caseStatement;
import static io.airlift.drift.codec.internal.compiler.byteCode.NamedParameterDefinition.arg;
import static io.airlift.drift.codec.internal.compiler.byteCode.ParameterizedType.type;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_UNION_ID;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

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
    private final ParameterizedType codecType;

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
        codecType = toCodecType(metadata);

        classDefinition = new ClassDefinition(
                a(PUBLIC, SUPER),
                codecType.getClassName(),
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

        // generate the byte code
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        classDefinition.getClassNode().accept(cw);
        byte[] byteCode = cw.toByteArray();

        // Run the asm verifier only in debug mode (prints a ton of info)
        if (debug) {
            ClassReader reader = new ClassReader(byteCode);
            CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
        }

        // load the class
        Class<?> codecClass = classLoader.defineClass(codecType.getClassName().replace('/', '.'), byteCode);
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
        FieldDefinition typeField = new FieldDefinition(a(PRIVATE, FINAL), "type", type(ThriftType.class));
        classDefinition.addField(typeField);

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

                FieldDefinition codecField = new FieldDefinition(a(PRIVATE, FINAL), fieldName, type(codec.getClass()));
                classDefinition.addField(codecField);
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
        //
        // declare the constructor
        MethodDefinition constructor = new MethodDefinition(
                a(PUBLIC),
                "<init>",
                type(void.class),
                parameters.getParameters());

        // invoke super (Object) constructor
        constructor.loadThis().invokeConstructor(type(Object.class));

        // this.foo = foo;
        for (FieldDefinition fieldDefinition : parameters.getFields()) {
            constructor.loadThis()
                    .loadVariable(fieldDefinition.getName())
                    .putField(codecType, fieldDefinition);
        }

        // return; (implicit)
        constructor.ret();

        classDefinition.addMethod(constructor);
    }

    /**
     * Defines the getType method which simply returns the value of the type field.
     */
    private void defineGetTypeMethod()
    {
        classDefinition.addMethod(
                new MethodDefinition(a(PUBLIC), "getType", type(ThriftType.class))
                        .loadThis()
                        .getField(codecType, typeField)
                        .retObject());
    }

    /**
     * Defines the read method for a struct.
     */
    private void defineReadStructMethod()
    {
        MethodDefinition read = new MethodDefinition(
                a(PUBLIC),
                "read",
                structType,
                arg("protocol", TProtocolReader.class)
        ).addException(Exception.class);

        // TProtocolReader reader = new TProtocolReader(protocol);
        read.addLocalVariable(type(ProtocolReader.class), "reader");
        read.newObject(ProtocolReader.class);
        read.dup();
        read.loadVariable("protocol");
        read.invokeConstructor(type(ProtocolReader.class), type(TProtocolReader.class));
        read.storeVariable("reader");

        // read all of the data in to local variables
        Map<Short, LocalVariableDefinition> structData = readFieldValues(read);

        // build the struct
        LocalVariableDefinition result = buildStruct(read, structData);

        // push instance on stack, and return it
        read.loadVariable(result).retObject();

        classDefinition.addMethod(read);
    }

    /**
     * Defines the code to read all of the data from the protocol into local variables.
     */
    private Map<Short, LocalVariableDefinition> readFieldValues(MethodDefinition read)
    {
        LocalVariableDefinition protocol = read.getLocalVariable("reader");

        // declare and init local variables here
        Map<Short, LocalVariableDefinition> structData = new TreeMap<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            LocalVariableDefinition variable = read.addInitializedLocalVariable(
                    toParameterizedType(field.getThriftType()),
                    "f_" + field.getName());
            structData.put(field.getId(), variable);
        }

        // protocol.readStructBegin();
        read.loadVariable(protocol).invokeVirtual(
                ProtocolReader.class,
                "readStructBegin",
                void.class);

        // while (protocol.nextField())
        read.visitLabel("while-begin");
        read.loadVariable(protocol).invokeVirtual(ProtocolReader.class, "nextField", boolean.class);
        read.ifZeroGoto("while-end");

        // switch (protocol.getFieldId())
        read.loadVariable(protocol).invokeVirtual(ProtocolReader.class, "getFieldId", short.class);
        List<CaseStatement> cases = new ArrayList<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            cases.add(caseStatement(field.getId(), field.getName() + "-field"));
        }
        read.switchStatement("default", cases);

        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            // case field.id:
            read.visitLabel(field.getName() + "-field");

            // push protocol
            read.loadVariable(protocol);

            // push ThriftTypeCodec for this field
            FieldDefinition codecField = codecFields.get(field.getId());
            if (codecField != null) {
                read.loadThis().getField(codecType, codecField);
            }

            // read value
            Method readMethod = getReadMethod(field.getThriftType());
            if (readMethod == null) {
                throw new IllegalArgumentException("Unsupported field type " + field.getThriftType().getProtocolType());
            }
            read.invokeVirtual(readMethod);

            // todo this cast should be based on readMethod return type and fieldType (or coercion type)
            // add cast if necessary
            if (needsCastAfterRead(field, readMethod)) {
                read.checkCast(toParameterizedType(field.getThriftType()));
            }

            // coerce the type
            if (field.getCoercion().isPresent()) {
                read.invokeStatic(field.getCoercion().get().getFromThrift());
            }

            // store protocol value
            read.storeVariable(structData.get(field.getId()));

            // go back to top of loop
            read.gotoLabel("while-begin");
        }

        // default case
        read.visitLabel("default")
                .loadVariable(protocol)
                .invokeVirtual(ProtocolReader.class, "skipFieldData", void.class)
                .gotoLabel("while-begin");

        // end of while loop
        read.visitLabel("while-end");

        // protocol.readStructEnd();
        read.loadVariable(protocol)
                .invokeVirtual(ProtocolReader.class, "readStructEnd", void.class);
        return structData;
    }

    /**
     * Defines the code to build the struct instance using the data in the local variables.
     */
    private LocalVariableDefinition buildStruct(MethodDefinition read, Map<Short, LocalVariableDefinition> structData)
    {
        // construct the instance and store it in the instance local variable
        LocalVariableDefinition instance = constructStructInstance(read, structData);

        // inject fields
        injectStructFields(read, instance, structData);

        // inject methods
        injectStructMethods(read, instance, structData);

        // invoke factory method if present
        invokeFactoryMethod(read, structData, instance);

        return instance;
    }

    /**
     * Defines the code to construct the struct (or builder) instance and stores it in a local
     * variable.
     */
    private LocalVariableDefinition constructStructInstance(MethodDefinition read, Map<Short, LocalVariableDefinition> structData)
    {
        LocalVariableDefinition instance = read.addLocalVariable(structType, "instance");

        // create the new instance (or builder)
        if (metadata.getBuilderClass() == null) {
            read.newObject(structType).dup();
        }
        else {
            read.newObject(metadata.getBuilderClass()).dup();
        }

        // invoke constructor
        ThriftConstructorInjection constructor = metadata.getConstructorInjection().get();
        // push parameters on stack
        for (ThriftParameterInjection parameter : constructor.getParameters()) {
            read.loadVariable(structData.get(parameter.getId()));

            // if there is a codec, replace null with the default null value from the codec
            FieldDefinition codecField = codecFields.get(parameter.getId());
            if (codecField != null) {
                read.dup();
                read.ifNotNullGoto(parameter.getName() + "-constructor-default");

                read.pop();
                read.loadThis().getField(codecType, codecField);
                read.invokeInterface(ThriftCodec.class, "getType", ThriftType.class);
                read.invokeVirtual(ThriftType.class, "getNullValue", Object.class);
                read.checkCast(structData.get(parameter.getId()).getType());

                read.visitLabel(parameter.getName() + "-constructor-default");
            }
        }
        // invoke constructor
        read.invokeConstructor(constructor.getConstructor())
                .storeVariable(instance);
        return instance;
    }

    /**
     * Defines the code to inject data into the struct public fields.
     */
    private void injectStructFields(MethodDefinition read, LocalVariableDefinition instance, Map<Short, LocalVariableDefinition> structData)
    {
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            injectField(read, field, instance, structData.get(field.getId()));
        }
    }

    /**
     * Defines the code to inject data into the struct methods.
     */
    private void injectStructMethods(MethodDefinition read, LocalVariableDefinition instance, Map<Short, LocalVariableDefinition> structData)
    {
        for (ThriftMethodInjection methodInjection : metadata.getMethodInjections()) {
            injectMethod(read, methodInjection, instance, structData);
        }
    }

    /**
     * Defines the read method for an union.
     */
    private void defineReadUnionMethod()
    {
        MethodDefinition read = new MethodDefinition(
                a(PUBLIC),
                "read",
                structType,
                arg("protocol", TProtocolReader.class)
        ).addException(Exception.class);

        // TProtocolReader reader = new TProtocolReader(protocol);
        read.addLocalVariable(type(ProtocolReader.class), "reader");
        read.newObject(ProtocolReader.class);
        read.dup();
        read.loadVariable("protocol");
        read.invokeConstructor(type(ProtocolReader.class), type(TProtocolReader.class));
        read.storeVariable("reader");

        // field id field.
        read.addInitializedLocalVariable(type(short.class), "fieldId");

        // read all of the data in to local variables
        Map<Short, LocalVariableDefinition> unionData = readSingleFieldValue(read);

        // build the struct
        LocalVariableDefinition result = buildUnion(read, unionData);

        // push instance on stack, and return it
        read.loadVariable(result).retObject();

        classDefinition.addMethod(read);
    }

    /**
     * Defines the code to read all of the data from the protocol into local variables.
     */
    private Map<Short, LocalVariableDefinition> readSingleFieldValue(MethodDefinition read)
    {
        LocalVariableDefinition protocol = read.getLocalVariable("reader");

        // declare and init local variables here
        Map<Short, LocalVariableDefinition> unionData = new TreeMap<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            LocalVariableDefinition variable = read.addInitializedLocalVariable(
                    toParameterizedType(field.getThriftType()),
                    "f_" + field.getName());
            unionData.put(field.getId(), variable);
        }

        // protocol.readStructBegin();
        read.loadVariable(protocol).invokeVirtual(
                ProtocolReader.class,
                "readStructBegin",
                void.class);

        // while (protocol.nextField())
        read.visitLabel("while-begin");
        read.loadVariable(protocol).invokeVirtual(ProtocolReader.class, "nextField", boolean.class);
        read.ifZeroGoto("while-end");

        // fieldId = protocol.getFieldId()
        read.loadVariable(protocol).invokeVirtual(ProtocolReader.class, "getFieldId", short.class);
        read.storeVariable("fieldId");

        // switch (fieldId)
        read.loadVariable("fieldId");

        List<CaseStatement> cases = new ArrayList<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            cases.add(caseStatement(field.getId(), field.getName() + "-field"));
        }
        read.switchStatement("default", cases);

        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            // case field.id:
            read.visitLabel(field.getName() + "-field");

            // push protocol
            read.loadVariable(protocol);

            // push ThriftTypeCodec for this field
            FieldDefinition codecField = codecFields.get(field.getId());
            if (codecField != null) {
                read.loadThis().getField(codecType, codecField);
            }

            // read value
            Method readMethod = getReadMethod(field.getThriftType());
            if (readMethod == null) {
                throw new IllegalArgumentException("Unsupported field type " + field.getThriftType().getProtocolType());
            }
            read.invokeVirtual(readMethod);

            // todo this cast should be based on readMethod return type and fieldType (or coercion type)
            // add cast if necessary
            if (needsCastAfterRead(field, readMethod)) {
                read.checkCast(toParameterizedType(field.getThriftType()));
            }

            // coerce the type
            if (field.getCoercion().isPresent()) {
                read.invokeStatic(field.getCoercion().get().getFromThrift());
            }

            // store protocol value
            read.storeVariable(unionData.get(field.getId()));

            // go back to top of loop
            read.gotoLabel("while-begin");
        }

        // default case
        read.visitLabel("default")
                .loadVariable(protocol)
                .invokeVirtual(ProtocolReader.class, "skipFieldData", void.class)
                .gotoLabel("while-begin");

        // end of while loop
        read.visitLabel("while-end");

        // protocol.readStructEnd();
        read.loadVariable(protocol)
                .invokeVirtual(ProtocolReader.class, "readStructEnd", void.class);

        return unionData;
    }

    /**
     * Defines the code to build the struct instance using the data in the local variables.
     */
    private LocalVariableDefinition buildUnion(MethodDefinition read, Map<Short, LocalVariableDefinition> unionData)
    {
        // construct the instance and store it in the instance local variable
        LocalVariableDefinition instance = constructUnionInstance(read);

        // switch (fieldId)
        read.loadVariable("fieldId");

        List<CaseStatement> cases = new ArrayList<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            cases.add(caseStatement(field.getId(), field.getName() + "-inject-field"));
        }
        read.switchStatement("inject-default", cases);

        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            // case field.id:
            read.visitLabel(field.getName() + "-inject-field");

            injectField(read, field, instance, unionData.get(field.getId()));

            if (field.getMethodInjection().isPresent()) {
                injectMethod(read, field.getMethodInjection().get(), instance, unionData);
            }

            read.gotoLabel("inject-default");
        }

        // default case
        read.visitLabel("inject-default");

        // find the @ThriftUnionId field
        ThriftFieldMetadata idField = getOnlyElement(metadata.getFields(THRIFT_UNION_ID));

        injectIdField(read, idField, instance, unionData);

        // invoke factory method if present
        invokeFactoryMethod(read, unionData, instance);

        return instance;
    }

    /**
     * Defines the code to construct the union (or builder) instance and stores it in a local
     * variable.
     */
    private LocalVariableDefinition constructUnionInstance(MethodDefinition read)
    {
        LocalVariableDefinition instance = read.addLocalVariable(structType, "instance");

        // create the new instance (or builder)
        if (metadata.getBuilderClass() == null) {
            read.newObject(structType).dup();
        }
        else {
            read.newObject(metadata.getBuilderClass()).dup();
        }

        // switch (fieldId)
        read.loadVariable("fieldId");

        List<CaseStatement> cases = new ArrayList<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            if (field.getConstructorInjection().isPresent()) {
                cases.add(caseStatement(field.getId(), field.getName() + "-id-field"));
            }
        }
        read.switchStatement("no-field-ctor", cases);

        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            if (field.getConstructorInjection().isPresent()) {
                // case fieldId:
                read.visitLabel(field.getName() + "-id-field");

                // Load the read value
                read.loadVariable("f_" + field.getName());
                read.invokeConstructor(field.getConstructorInjection().get().getConstructor())
                        .storeVariable(instance)
                        .gotoLabel("instance-ok");
            }
        }

        read.visitLabel("no-field-ctor");

        // No args c'tor present.
        if (metadata.getConstructorInjection().isPresent()) {
            ThriftConstructorInjection constructor = metadata.getConstructorInjection().get();
            // invoke constructor
            read.invokeConstructor(constructor.getConstructor())
                    .storeVariable(instance);
        }
        else {
            read.pop() // get rid of the half-constructed element
                    .loadConstant(metadata.getStructClass())
                    .loadVariable("fieldId")
                    .invokeStatic(DriftBytecodeHelper.NO_CONSTRUCTOR_FOUND)
                    .throwException();
        }
        read.visitLabel("instance-ok");

        return instance;
    }

    private void injectField(MethodDefinition read, ThriftFieldMetadata field, LocalVariableDefinition instance, LocalVariableDefinition sourceVariable)
    {
        for (ThriftInjection injection : field.getInjections()) {
            if (injection instanceof ThriftFieldInjection) {
                ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;

                // if field is an Object && field != null
                if (!fieldInjection.getField().getType().isPrimitive()) {
                    read.loadVariable(sourceVariable)
                            .ifNullGoto("field_is_null_" + field.getName());
                }

                // write value
                read.loadVariable(instance)
                        .loadVariable(sourceVariable)
                        .putField(fieldInjection.getField());

                // else do nothing
                if (!fieldInjection.getField().getType().isPrimitive()) {
                    read.visitLabel("field_is_null_" + field.getName());
                }
            }
        }
    }

    private void injectMethod(MethodDefinition read, ThriftMethodInjection methodInjection, LocalVariableDefinition instance, Map<Short, LocalVariableDefinition> structData)
    {
        // if any parameter is non-null, invoke the method
        String methodName = methodInjection.getMethod().toGenericString();
        for (ThriftParameterInjection parameter : methodInjection.getParameters()) {
            if (!isParameterTypeJavaPrimitive(parameter)) {
                read.loadVariable(structData.get(parameter.getId()))
                        .ifNotNullGoto("invoke_" + methodName);
            }
            else {
                read.gotoLabel("invoke_" + methodName);
            }
        }
        read.gotoLabel("skip_invoke_" + methodName);

        // invoke the method
        read.visitLabel("invoke_" + methodName)
                .loadVariable(instance);

        // push parameters on stack
        for (ThriftParameterInjection parameter : methodInjection.getParameters()) {
            read.loadVariable(structData.get(parameter.getId()));
        }

        // invoke the method
        read.invokeVirtual(methodInjection.getMethod());

        // if method has a return, we need to pop it off the stack
        if (methodInjection.getMethod().getReturnType() != void.class) {
            read.pop();
        }

        // skip invocation
        read.visitLabel("skip_invoke_" + methodName);
    }

    /**
     * Defines the code that calls the builder factory method.
     */
    private void invokeFactoryMethod(MethodDefinition read, Map<Short, LocalVariableDefinition> structData, LocalVariableDefinition instance)
    {
        if (metadata.getBuilderMethod().isPresent()) {
            ThriftMethodInjection builderMethod = metadata.getBuilderMethod().get();
            read.loadVariable(instance);

            // push parameters on stack
            for (ThriftParameterInjection parameter : builderMethod.getParameters()) {
                read.loadVariable(structData.get(parameter.getId()));
            }

            // invoke the method
            read.invokeVirtual(builderMethod.getMethod())
                    .storeVariable(instance);
        }
    }

    private void injectIdField(MethodDefinition read, ThriftFieldMetadata field, LocalVariableDefinition instance, Map<Short, LocalVariableDefinition> structData)
    {
        for (ThriftInjection injection : field.getInjections()) {
            if (injection instanceof ThriftFieldInjection) {
                ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;

                // if field is an Object && field != null
                if (!isProtocolTypeJavaPrimitive(field)) {
                    read.loadVariable("fieldId")
                            .ifNullGoto("field_is_null_fieldId");
                }

                // write value
                read.loadVariable(instance)
                        .loadVariable("fieldId")
                        .putField(fieldInjection.getField());

                // else do nothing
                if (!isProtocolTypeJavaPrimitive(field)) {
                    read.visitLabel("field_is_null_fieldId");
                }
            }
        }
    }

    /**
     * Define the write method.
     */
    private void defineWriteStructMethod()
    {
        MethodDefinition write = new MethodDefinition(
                a(PUBLIC),
                "write",
                null,
                arg("struct", structType),
                arg("protocol", TProtocolWriter.class));

        classDefinition.addMethod(write);

        // TProtocolReader reader = new TProtocolReader(protocol);
        write.addLocalVariable(type(ProtocolWriter.class), "writer");
        write.newObject(ProtocolWriter.class);
        write.dup();
        write.loadVariable("protocol");
        write.invokeConstructor(type(ProtocolWriter.class), type(TProtocolWriter.class));
        write.storeVariable("writer");

        LocalVariableDefinition protocol = write.getLocalVariable("writer");

        // protocol.writeStructBegin("bonk");
        write.loadVariable(protocol)
                .loadConstant(metadata.getStructName())
                .invokeVirtual(ProtocolWriter.class, "writeStructBegin", void.class, String.class);

        // write fields
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            writeField(write, protocol, field);
        }

        write.loadVariable(protocol)
                .invokeVirtual(ProtocolWriter.class, "writeStructEnd", void.class);

        write.ret();
    }

    /**
     * Define the write method.
     */
    private void defineWriteUnionMethod()
    {
        MethodDefinition write = new MethodDefinition(
                a(PUBLIC),
                "write",
                null,
                arg("struct", structType),
                arg("protocol", TProtocolWriter.class));

        classDefinition.addMethod(write);

        // TProtocolWriter writer = new TProtocolWriter(protocol);
        write.addLocalVariable(type(ProtocolWriter.class), "writer");
        write.newObject(ProtocolWriter.class);
        write.dup();
        write.loadVariable("protocol");
        write.invokeConstructor(type(ProtocolWriter.class), type(TProtocolWriter.class));
        write.storeVariable("writer");

        LocalVariableDefinition protocol = write.getLocalVariable("writer");

        // protocol.writeStructBegin("bonk");
        write.loadVariable(protocol)
                .loadConstant(metadata.getStructName())
                .invokeVirtual(ProtocolWriter.class, "writeStructBegin", void.class, String.class);

        // find the @ThriftUnionId field
        ThriftFieldMetadata idField = getOnlyElement(metadata.getFields(THRIFT_UNION_ID));

        // load its value
        loadFieldValue(write, idField);

        // switch(fieldId)
        List<CaseStatement> cases = new ArrayList<>();
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            cases.add(caseStatement(field.getId(), field.getName() + "-write-field"));
        }
        write.switchStatement("default-write", cases);

        // write fields
        for (ThriftFieldMetadata field : metadata.getFields(THRIFT_FIELD)) {
            write.visitLabel(field.getName() + "-write-field");
            writeField(write, protocol, field);
            write.gotoLabel("default-write");
        }

        write.visitLabel("default-write")
                .loadVariable(protocol)
                .invokeVirtual(ProtocolWriter.class, "writeStructEnd", void.class);

        write.ret();
    }

    private void writeField(MethodDefinition write, LocalVariableDefinition protocol, ThriftFieldMetadata field)
    {
        // push protocol
        write.loadVariable(protocol);

        // push (String) field.name
        write.loadConstant(field.getName());

        // push (short) field.id
        write.loadConstant(field.getId());

        // push ThriftTypeCodec for this field
        FieldDefinition codecField = codecFields.get(field.getId());
        if (codecField != null) {
            write.loadThis().getField(codecType, codecField);
        }

        // push field value
        loadFieldValue(write, field);

        // if field value is null, don't coerce or write the field
        if (!isFieldTypeJavaPrimitive(field)) {
            // ifNullGoto consumes the top of the stack, so we need to duplicate the value
            write.dup();
            write.ifNullGoto("field_is_null_" + field.getName());
        }

        // coerce value
        if (field.getCoercion().isPresent()) {
            write.invokeStatic(field.getCoercion().get().getToThrift());

            // if coerced value is null, don't write the field
            if (!isProtocolTypeJavaPrimitive(field)) {
                write.dup();
                write.ifNullGoto("field_is_null_" + field.getName());
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
            write.gotoLabel("field_end_" + field.getName());

            // cleanup stack for null field value
            write.visitLabel("field_is_null_" + field.getName());
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

            write.visitLabel("field_end_" + field.getName());
        }
    }

    private void loadFieldValue(MethodDefinition write, ThriftFieldMetadata field)
    {
        write.loadVariable("struct");
        if (field.getExtraction().isPresent()) {
            ThriftExtraction extraction = field.getExtraction().get();
            if (extraction instanceof ThriftFieldExtractor) {
                ThriftFieldExtractor fieldExtractor = (ThriftFieldExtractor) extraction;
                write.getField(fieldExtractor.getField());
                if (fieldExtractor.isGeneric()) {
                    write.checkCast(type(fieldExtractor.getType()));
                }
            }
            else if (extraction instanceof ThriftMethodExtractor) {
                ThriftMethodExtractor methodExtractor = (ThriftMethodExtractor) extraction;
                write.invokeVirtual(methodExtractor.getMethod());
                if (methodExtractor.isGeneric()) {
                    write.checkCast(type(methodExtractor.getType()));
                }
            }
        }
    }

    /**
     * Defines the generics bridge method with untyped args to the type specific read method.
     */
    private void defineReadBridgeMethod()
    {
        classDefinition.addMethod(
                new MethodDefinition(a(PUBLIC, BRIDGE, SYNTHETIC), "read", type(Object.class), arg("protocol", TProtocolReader.class))
                        .addException(Exception.class)
                        .loadThis()
                        .loadVariable("protocol")
                        .invokeVirtual(codecType, "read", structType, type(TProtocolReader.class))
                        .retObject());
    }

    /**
     * Defines the generics bridge method with untyped args to the type specific write method.
     */
    private void defineWriteBridgeMethod()
    {
        classDefinition.addMethod(
                new MethodDefinition(a(PUBLIC, BRIDGE, SYNTHETIC), "write", null, arg("struct", Object.class), arg("protocol", TProtocolWriter.class))
                        .addException(Exception.class)
                        .loadThis()
                        .loadVariable("struct", structType)
                        .loadVariable("protocol")
                        .invokeVirtual(
                                codecType,
                                "write",
                                type(void.class),
                                structType,
                                type(TProtocolWriter.class))
                        .ret());
    }

    private boolean isParameterTypeJavaPrimitive(ThriftParameterInjection parameter)
    {
        return isJavaPrimitive(TypeToken.of(parameter.getJavaType()));
    }

    private boolean isFieldTypeJavaPrimitive(ThriftFieldMetadata field)
    {
        return isJavaPrimitive(TypeToken.of(field.getThriftType().getJavaType()));
    }

    private boolean isProtocolTypeJavaPrimitive(ThriftFieldMetadata field)
    {
        if (field.getThriftType().isCoerced()) {
            return isJavaPrimitive(TypeToken.of(field.getThriftType().getUncoercedType().getJavaType()));
        }
        else {
            return isJavaPrimitive(TypeToken.of(field.getThriftType().getJavaType()));
        }
    }

    private boolean isJavaPrimitive(TypeToken<?> typeToken)
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

    private boolean needsCodec(ThriftFieldMetadata fieldMetadata)
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

    private ParameterizedType toCodecType(ThriftStructMetadata metadata)
    {
        return type(PACKAGE + "/" + type(metadata.getStructClass()).getClassName() + "Codec");
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
            return values.toArray(new Object[values.size()]);
        }

        public List<NamedParameterDefinition> getParameters()
        {
            return fields.stream()
                    .map(field -> arg(field.getName(), field.getType()))
                    .collect(toList());
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
