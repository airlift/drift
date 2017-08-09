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
package io.airlift.drift.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.annotations.ThriftException;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftIdlAnnotation;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftStruct;
import io.airlift.drift.codec.metadata.DefaultThriftTypeReference;
import io.airlift.drift.codec.metadata.ThriftCatalog;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftInjection;
import io.airlift.drift.codec.metadata.ThriftParameterInjection;
import io.airlift.drift.codec.metadata.ThriftType;
import org.apache.thrift.TException;

import javax.annotation.concurrent.Immutable;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;
import static io.airlift.drift.codec.metadata.ReflectionHelper.extractParameterNames;
import static java.util.Objects.requireNonNull;

@Immutable
class ThriftMethodMetadata
{
    private final String name;
    private final ThriftType returnType;
    private final List<ThriftFieldMetadata> parameters;
    private final Method method;
    private final ImmutableMap<Short, ThriftType> exceptions;
    private final boolean oneway;

    public ThriftMethodMetadata(Method method, ThriftCatalog catalog)
    {
        requireNonNull(method, "method is null");
        requireNonNull(catalog, "catalog is null");

        this.method = method;

        ThriftMethod thriftMethod = method.getAnnotation(ThriftMethod.class);
        Preconditions.checkArgument(thriftMethod != null, "Method is not annotated with @ThriftMethod");

        Preconditions.checkArgument(!Modifier.isStatic(method.getModifiers()), "Method %s is static", method.toGenericString());

        if (thriftMethod.value().isEmpty()) {
            name = method.getName();
        }
        else {
            name = thriftMethod.value();
        }

        returnType = catalog.getThriftType(method.getGenericReturnType());

        ImmutableList.Builder<ThriftFieldMetadata> builder = ImmutableList.builder();
        Type[] parameterTypes = method.getGenericParameterTypes();
        List<String> parameterNames = extractParameterNames(method);
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        for (int index = 0; index < parameterTypes.length; index++) {
            ThriftField thriftField = null;
            for (Annotation annotation : parameterAnnotations[index]) {
                if (annotation instanceof ThriftField) {
                    thriftField = (ThriftField) annotation;
                    break;
                }
            }

            short parameterId = Short.MIN_VALUE;
            boolean isLegacyId = false;
            String parameterName = null;
            Map<String, String> parameterIdlAnnotations = null;
            Requiredness parameterRequiredness = Requiredness.UNSPECIFIED;
            if (thriftField != null) {
                parameterId = thriftField.value();
                isLegacyId = thriftField.isLegacyId();
                parameterRequiredness = thriftField.requiredness();
                ImmutableMap.Builder<String, String> idlAnnotationsBuilder = ImmutableMap.builder();
                for (ThriftIdlAnnotation idlAnnotation : thriftField.idlAnnotations()) {
                    idlAnnotationsBuilder.put(idlAnnotation.key(), idlAnnotation.value());
                }
                parameterIdlAnnotations = idlAnnotationsBuilder.build();

                if (!thriftField.name().isEmpty()) {
                    parameterName = thriftField.name();
                }
            }
            if (parameterId == Short.MIN_VALUE) {
                parameterId = (short) (index + 1);
            }
            if (parameterName == null) {
                parameterName = parameterNames.get(index);
            }

            Type parameterType = parameterTypes[index];

            ThriftType thriftType = catalog.getThriftType(parameterType);

            ThriftInjection parameterInjection = new ThriftParameterInjection(parameterId, parameterName, index, parameterType);

            if (parameterRequiredness == Requiredness.UNSPECIFIED) {
                // There is only one field injection used to build metadata for method parameters, and if a
                // single injection point has UNSPECIFIED requiredness, that resolves to NONE.
                parameterRequiredness = Requiredness.NONE;
            }

            ThriftFieldMetadata fieldMetadata = new ThriftFieldMetadata(
                    parameterId,
                    isLegacyId,
                    false,
                    parameterRequiredness,
                    parameterIdlAnnotations,
                    new DefaultThriftTypeReference(thriftType),
                    parameterName,
                    THRIFT_FIELD,
                    ImmutableList.of(parameterInjection),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            builder.add(fieldMetadata);
        }
        parameters = builder.build();

        exceptions = buildExceptionMap(catalog, thriftMethod);

        this.oneway = thriftMethod.oneway();
    }

    public String getName()
    {
        return name;
    }

    public ThriftType getReturnType()
    {
        return returnType;
    }

    public List<ThriftFieldMetadata> getParameters()
    {
        return parameters;
    }

    public Map<Short, ThriftType> getExceptions()
    {
        return exceptions;
    }

    public Method getMethod()
    {
        return method;
    }

    public boolean getOneway()
    {
        return oneway;
    }

    private ImmutableMap<Short, ThriftType> buildExceptionMap(ThriftCatalog catalog, ThriftMethod thriftMethod)
    {
        ImmutableMap.Builder<Short, ThriftType> exceptions = ImmutableMap.builder();
        Set<Type> exceptionTypes = new HashSet<>();
        int customExceptionCount = 0;

        if (thriftMethod.exception().length > 0) {
            for (ThriftException thriftException : thriftMethod.exception()) {
                exceptions.put(thriftException.id(), catalog.getThriftType(thriftException.type()));
                checkArgument(exceptionTypes.add(thriftException.type()), "ThriftMethod exception list contains more than one value for %s", thriftException.type());
            }
        }

        for (Class<?> exceptionClass : method.getExceptionTypes()) {
            if (exceptionClass.isAssignableFrom(TException.class)) {
                // the built-in exception types don't need special treatment
                continue;
            }

            if (exceptionClass.isAnnotationPresent(ThriftStruct.class)) {
                customExceptionCount++;

                if (!exceptionTypes.contains(exceptionClass)) {
                    // there is no ordering guarantee for exception types,
                    // so we can only infer the id if there is a single custom exception
                    checkArgument(customExceptionCount <= 1, "ThriftMethod annotation must declare exception mapping when more than one custom exception is thrown");
                    exceptions.put((short) 1, catalog.getThriftType(exceptionClass));
                }
            }
        }

        return exceptions.build();
    }

    public boolean isAsync()
    {
        Type returnType = method.getGenericReturnType();
        Class<?> rawType = TypeToken.of(returnType).getRawType();
        return ListenableFuture.class.isAssignableFrom(rawType);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ThriftMethodMetadata that = (ThriftMethodMetadata) o;
        return oneway == that.oneway &&
                Objects.equals(name, that.name) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(parameters, that.parameters) &&
                Objects.equals(method, that.method) &&
                Objects.equals(exceptions, that.exceptions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, returnType, parameters, method, exceptions, oneway);
    }
}
