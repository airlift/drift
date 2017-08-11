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
package io.airlift.drift.idl.generator;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.drift.annotations.ThriftService;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.codec.ThriftProtocolType;
import io.airlift.drift.codec.metadata.FieldKind;
import io.airlift.drift.codec.metadata.MetadataErrorException;
import io.airlift.drift.codec.metadata.MetadataErrors.Monitor;
import io.airlift.drift.codec.metadata.MetadataWarningException;
import io.airlift.drift.codec.metadata.ThriftCatalog;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftMethodMetadata;
import io.airlift.drift.codec.metadata.ThriftServiceMetadata;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.codec.metadata.ThriftTypeReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.io.Files.getNameWithoutExtension;
import static io.airlift.drift.codec.metadata.ReflectionHelper.getEffectiveClassAnnotations;
import static io.airlift.drift.idl.generator.ThriftIdlRenderer.renderThriftIdl;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ThriftIdlGenerator
{
    private static final Set<ThriftType> BUILT_IN_TYPES = ImmutableSet.<ThriftType>builder()
            .add(ThriftType.BOOL)
            .add(ThriftType.BYTE)
            .add(ThriftType.I16)
            .add(ThriftType.I32)
            .add(ThriftType.I64)
            .add(ThriftType.DOUBLE)
            .add(ThriftType.STRING)
            .add(ThriftType.BINARY)
            .add(new ThriftType(ThriftType.BOOL, Boolean.class))
            .add(new ThriftType(ThriftType.BYTE, Byte.class))
            .add(new ThriftType(ThriftType.I16, Short.class))
            .add(new ThriftType(ThriftType.I32, Integer.class))
            .add(new ThriftType(ThriftType.I64, Long.class))
            .add(new ThriftType(ThriftType.DOUBLE, Double.class))
            .add(new ThriftType(ThriftType.STRING, String.class))
            .add(new ThriftType(ThriftType.BINARY, byte[].class))
            .build();

    private final ClassLoader classLoader;
    private final Consumer<String> verboseLogger;
    private final ThriftCodecManager codecManager;
    private final String defaultPackage;
    private final Map<Object, String> includes = new HashMap<>();
    private final Set<ThriftType> usedIncludedTypes = new HashSet<>();
    private final Map<String, String> namespaces;

    private Set<ThriftType> knownTypes = new HashSet<>(BUILT_IN_TYPES);
    private ThriftTypeRenderer typeRenderer = new ThriftTypeRenderer(ImmutableMap.of());
    private List<ThriftType> thriftTypes = new ArrayList<>();
    private List<ThriftServiceMetadata> thriftServices = new ArrayList<>();
    private boolean recursive;

    public ThriftIdlGenerator(ThriftIdlGeneratorConfig config)
    {
        this(config, firstNonNull(Thread.currentThread().getContextClassLoader(), ClassLoader.getSystemClassLoader()));
    }

    public ThriftIdlGenerator(ThriftIdlGeneratorConfig config, ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");

        Monitor monitor = createMonitor(config.getErrorLogger(), config.getWarningLogger());
        this.codecManager = new ThriftCodecManager(new ThriftCatalog(monitor));

        this.verboseLogger = config.getVerboseLogger();
        String defaultPackage = config.getDefaultPackage();

        if (defaultPackage.isEmpty()) {
            this.defaultPackage = "";
        }
        else {
            this.defaultPackage = defaultPackage + ".";
        }

        Map<String, String> paramIncludeMap = config.getIncludes();
        for (Map.Entry<String, String> entry : paramIncludeMap.entrySet()) {
            Class<?> clazz = load(entry.getKey());
            if (clazz == null) {
                continue;
            }

            Object result = convertToThrift(clazz);
            this.includes.put(result, entry.getValue());
        }

        this.namespaces = config.getNamespaces();
        this.recursive = config.isRecursive();
    }

    public String generate(Iterable<String> inputs)
    {
        for (String className : inputs) {
            Object result = convertToThrift(load(className));
            if (result instanceof ThriftType) {
                thriftTypes.add((ThriftType) result);
            }
            else {
                thriftServices.add((ThriftServiceMetadata) result);
            }
            // if the class we just loaded was also in the include map, remove it from there
            includes.remove(result);
        }

        if (!verify()) {
            throw new ThriftIdlGeneratorException("Errors found during verification.");
        }

        return generate();
    }

    private String getFullClassName(String className)
    {
        if (className.indexOf('.') == -1) {
            return defaultPackage + className;
        }
        return className;
    }

    @SuppressWarnings("NonShortCircuitBooleanExpression")
    @SuppressFBWarnings("NS_DANGEROUS_NON_SHORT_CIRCUIT")
    private boolean verify()
    {
        if (recursive) {
            // Call verifyStruct and verifyService until the lists of discovered types and services stop changing.
            // This populates the list with all transitive dependencies of the input types/services to be fed into the
            // topological sort of verifyTypes() and verifyServices().
            while (true) {
                int size = thriftTypes.size();
                for (ThriftType type : thriftTypes) {
                    verifyStruct(type, true);
                }
                if (size != thriftTypes.size()) {
                    break;
                }
            }

            while (true) {
                int size = thriftServices.size();
                for (ThriftServiceMetadata service : thriftServices) {
                    verifyService(service, true);
                }
                if (size != thriftServices.size()) {
                    break;
                }
            }

            recursive = false;
            usedIncludedTypes.clear();
            knownTypes = new HashSet<>(BUILT_IN_TYPES);
        }
        return verifyTypes() & verifyServices();
    }

    // verifies that all types are known (in thriftTypes or in include map)
    // and does a topological sort of thriftTypes in dependency order
    private boolean verifyTypes()
    {
        SuccessAndResult<ThriftType> output = topologicalSort(thriftTypes, type -> {
            ThriftProtocolType proto = type.getProtocolType();
            if (proto == ThriftProtocolType.ENUM || proto == ThriftProtocolType.STRUCT) {
                return verifyStruct(type, true);
            }
            throw new VerifyException("Top-level non-enum and non-struct?");
        });
        if (output.isSuccess()) {
            thriftTypes = output.getResult();
            return true;
        }
        for (ThriftType type : output.getResult()) {
            // we know it's gonna fail, we just want the precise error message
            verifyStruct(type, false);
        }
        return false;
    }

    private boolean verifyServices()
    {
        SuccessAndResult<ThriftServiceMetadata> output = topologicalSort(thriftServices, metadata -> verifyService(metadata, true));
        if (output.isSuccess()) {
            thriftServices = output.getResult();
            return true;
        }
        for (ThriftServiceMetadata s : output.getResult()) {
            // we know it's gonna fail, we just want the precise error message
            verifyService(s, false);
        }
        return false;
    }

    private static class SuccessAndResult<T>
    {
        private final boolean success;
        private final List<T> result;

        SuccessAndResult(boolean success, List<T> result)
        {
            this.success = success;
            this.result = ImmutableList.copyOf(result);
        }

        public boolean isSuccess()
        {
            return success;
        }

        public List<T> getResult()
        {
            return result;
        }
    }

    private static <T> SuccessAndResult<T> topologicalSort(List<T> list, Predicate<T> isKnown)
    {
        List<T> remaining = list;
        List<T> newList = new ArrayList<>();
        int prevSize = 0;
        while (prevSize != remaining.size()) {
            prevSize = remaining.size();
            List<T> bad = new ArrayList<>();
            for (T value : remaining) {
                if (isKnown.test(value)) {
                    newList.add(value);
                }
                else {
                    bad.add(value);
                }
            }
            remaining = bad;
        }
        if (prevSize == 0) {
            return new SuccessAndResult<>(true, newList);
        }
        return new SuccessAndResult<>(false, remaining);
    }

    private boolean verifyService(ThriftServiceMetadata service, boolean quiet)
    {
        boolean ok = true;

        for (Map.Entry<String, ThriftMethodMetadata> method : service.getMethods().entrySet()) {
            for (ThriftFieldMetadata field : method.getValue().getParameters()) {
                if (!verifyField(field.getThriftType())) {
                    ok = false;
                    if (!quiet) {
                        throw new ThriftIdlGeneratorException(format("Unknown argument type %s in %s.%s", typeName(field.getThriftType()), service.getName(), method.getKey()));
                    }
                }
            }

            for (ThriftType exception : method.getValue().getExceptions().values()) {
                if (!verifyField(exception)) {
                    ok = false;
                    if (!quiet) {
                        throw new ThriftIdlGeneratorException(format("Unknown exception type %s in %s.%s", typeName(exception), service.getName(), method.getKey()));
                    }
                }
            }

            if (!method.getValue().getReturnType().equals(ThriftType.VOID) && !verifyField(method.getValue().getReturnType())) {
                ok = false;
                if (!quiet) {
                    throw new ThriftIdlGeneratorException(format("Unknown return type %s in %s.%s", typeName(method.getValue().getReturnType()), service.getName(), method.getKey()));
                }
            }
        }

        return ok;
    }

    private boolean verifyElementType(ThriftTypeReference type)
    {
        if (!recursive && type.isRecursive()) {
            return true;
        }
        return verifyField(type.get());
    }

    @SuppressWarnings("NonShortCircuitBooleanExpression")
    @SuppressFBWarnings("NS_DANGEROUS_NON_SHORT_CIRCUIT")
    private boolean verifyField(ThriftType type)
    {
        ThriftProtocolType proto = type.getProtocolType();
        if (proto == ThriftProtocolType.SET || proto == ThriftProtocolType.LIST) {
            return verifyElementType(type.getValueTypeReference());
        }
        if (proto == ThriftProtocolType.MAP) {
            return verifyElementType(type.getKeyTypeReference()) & verifyElementType(type.getValueTypeReference());
        }

        if (knownTypes.contains(type)) {
            return true;
        }

        if (includes.containsKey(type)) {
            usedIncludedTypes.add(type);
            return true;
        }

        if (recursive) {
            // recursive but type is unknown - add it to the list and recurse
            thriftTypes.add(type);
            return verifyStruct(type, true);
        }
        return false;
    }

    private boolean verifyStruct(ThriftType type, boolean quiet)
    {
        if (type.getProtocolType() == ThriftProtocolType.ENUM) {
            knownTypes.add(type);
            return true;
        }
        ThriftStructMetadata metadata = type.getStructMetadata();
        boolean ok = true;

        knownTypes.add(type);

        for (ThriftFieldMetadata fieldMetadata : metadata.getFields(FieldKind.THRIFT_FIELD)) {
            if (!recursive && fieldMetadata.isTypeReferenceRecursive()) {
                continue;
            }

            boolean fieldOk = verifyField(fieldMetadata.getThriftType());
            if (!fieldOk) {
                ok = false;
                if (!quiet) {
                    throw new ThriftIdlGeneratorException(format("Unknown type %s in %s.%s", typeName(fieldMetadata.getThriftType()), metadata.getStructName(), fieldMetadata.getName()));
                }
            }
        }

        if (!ok) {
            knownTypes.remove(type);
        }
        return ok;
    }

    private Class<?> load(String className)
    {
        className = getFullClassName(className);
        try {
            return classLoader.loadClass(className);
        }
        catch (ClassNotFoundException e) {
            throw new ThriftIdlGeneratorException("Class not found: " + className);
        }
    }

    private Object convertToThrift(Class<?> clazz)
    {
        try {
            return getThriftType(clazz);
        }
        catch (MetadataErrorException e) {
            throw new ThriftIdlGeneratorException(e);
        }
    }

    private Object getThriftType(Class<?> clazz)
    {
        Set<ThriftService> serviceAnnotations = getEffectiveClassAnnotations(clazz, ThriftService.class);
        if (serviceAnnotations.isEmpty()) {
            ThriftType thriftType = codecManager.getCatalog().getThriftType(clazz);
            verboseLogger.accept("Found Thrift type: " + typeName(thriftType));
            return thriftType;
        }

        ThriftServiceMetadata serviceMetadata = new ThriftServiceMetadata(clazz, codecManager.getCatalog());
        verboseLogger.accept("Found Thrift service: " + clazz.getSimpleName());
        return serviceMetadata;
    }

    private String generate()
    {
        ImmutableMap.Builder<ThriftType, String> typesBuilder = ImmutableMap.builder();
        ImmutableSet.Builder<String> includesBuilder = ImmutableSet.builder();
        for (ThriftType type : usedIncludedTypes) {
            String filename = includes.get(type);
            includesBuilder.add(filename);
            typesBuilder.put(type, getNameWithoutExtension(filename));
        }
        typeRenderer = new ThriftTypeRenderer(typesBuilder.build());
        return renderThriftIdl(namespaces, includesBuilder.build(), thriftTypes, thriftServices, typeRenderer);
    }

    private String typeName(ThriftType type)
    {
        return typeRenderer.toString(type);
    }

    private static Monitor createMonitor(Consumer<String> errorLogger, Consumer<String> warningLogger)
    {
        return new Monitor()
        {
            @Override
            public void onError(MetadataErrorException e)
            {
                errorLogger.accept(e.getMessage());
            }

            @Override
            public void onWarning(MetadataWarningException e)
            {
                warningLogger.accept(e.getMessage());
            }
        };
    }
}
