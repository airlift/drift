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
package io.airlift.drift.codec.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.drift.TException;
import io.airlift.drift.annotations.ThriftException;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftHeader;
import io.airlift.drift.annotations.ThriftId;
import io.airlift.drift.annotations.ThriftIdlAnnotation;
import io.airlift.drift.annotations.ThriftMethod;
import io.airlift.drift.annotations.ThriftStruct;

import javax.annotation.concurrent.Immutable;

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness;
import static io.airlift.drift.codec.metadata.FieldKind.THRIFT_FIELD;
import static io.airlift.drift.codec.metadata.ReflectionHelper.extractParameterNames;
import static io.airlift.drift.codec.metadata.ThriftCatalog.getThriftDocumentation;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

@Immutable
public class ThriftMethodMetadata
{
    private final String name;
    private final ThriftType returnType;
    private final List<ThriftFieldMetadata> parameters;
    private final Set<ThriftHeaderParameter> headerParameters;
    private final Method method;
    private final ImmutableMap<Short, ThriftType> exceptions;
    private final boolean oneway;
    private final boolean idempotent;
    private final List<String> documentation;

    public ThriftMethodMetadata(Method method, ThriftCatalog catalog)
    {
        requireNonNull(method, "method is null");
        requireNonNull(catalog, "catalog is null");

        this.method = method;

        ThriftMethod thriftMethod = method.getAnnotation(ThriftMethod.class);
        checkArgument(thriftMethod != null, "Method is not annotated with @ThriftMethod");

        checkArgument(!Modifier.isStatic(method.getModifiers()), "Method %s is static", method.toGenericString());

        if (thriftMethod.value().isEmpty()) {
            name = method.getName();
        }
        else {
            name = thriftMethod.value();
        }

        returnType = catalog.getThriftType(method.getGenericReturnType());

        ImmutableList.Builder<ThriftFieldMetadata> thriftParameterBuilder = ImmutableList.builder();
        ImmutableSet.Builder<ThriftHeaderParameter> headerParameterBuilder = ImmutableSet.builder();
        Type[] parameterTypes = method.getGenericParameterTypes();
        List<String> parameterNames = extractParameterNames(method);
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();
        short nextThriftFieldId = 1;
        for (int parameterIndex = 0; parameterIndex < parameterTypes.length; parameterIndex++) {
            ThriftField thriftField = Stream.of(parameterAnnotations[parameterIndex])
                    .filter(ThriftField.class::isInstance)
                    .map(ThriftField.class::cast)
                    .findAny()
                    .orElse(null);

            ThriftHeader thriftHeader = Stream.of(parameterAnnotations[parameterIndex])
                    .filter(ThriftHeader.class::isInstance)
                    .map(ThriftHeader.class::cast)
                    .findAny()
                    .orElse(null);
            if (thriftHeader != null) {
                checkArgument(
                        thriftField == null,
                        "ThriftMethod [%s] parameter %s must not be annotated with both @ThriftField and @ThriftHeader",
                        methodName(method),
                        parameterIndex);

                checkArgument(
                        parameterTypes[parameterIndex] == String.class,
                        "ThriftMethod [%s] parameter %s annotated with @ThriftHeader must be a String",
                        methodName(method),
                        parameterIndex);

                String headerName = thriftHeader.value();
                checkArgument(
                        !headerName.isEmpty(),
                        "ThriftMethod [%s] parameter %s @ThriftHeader.name must not be empty",
                        methodName(method),
                        parameterIndex);

                headerParameterBuilder.add(new ThriftHeaderParameter(parameterIndex, headerName));
                continue;
            }

            short thriftFieldId = Short.MIN_VALUE;
            boolean isLegacyId = false;
            String parameterName = null;
            Map<String, String> parameterIdlAnnotations = null;
            Requiredness parameterRequiredness = Requiredness.UNSPECIFIED;
            if (thriftField != null) {
                thriftFieldId = thriftField.value();
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

            if (thriftFieldId == Short.MIN_VALUE) {
                thriftFieldId = nextThriftFieldId;
            }
            nextThriftFieldId++;

            if (parameterName == null) {
                parameterName = parameterNames.get(parameterIndex);
            }

            Type parameterType = parameterTypes[parameterIndex];

            ThriftType thriftType = catalog.getThriftType(parameterType);

            ThriftInjection parameterInjection = new ThriftParameterInjection(thriftFieldId, parameterName, parameterIndex, parameterType);

            if (parameterRequiredness == Requiredness.UNSPECIFIED) {
                // There is only one field injection used to build metadata for method parameters, and if a
                // single injection point has UNSPECIFIED requiredness, that resolves to NONE.
                parameterRequiredness = Requiredness.NONE;
            }

            ThriftFieldMetadata fieldMetadata = new ThriftFieldMetadata(
                    thriftFieldId,
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
            thriftParameterBuilder.add(fieldMetadata);
        }
        parameters = thriftParameterBuilder.build();
        headerParameters = headerParameterBuilder.build();

        exceptions = buildExceptionMap(catalog, thriftMethod);

        this.oneway = thriftMethod.oneway();
        this.idempotent = thriftMethod.idempotent();

        documentation = getThriftDocumentation(method);
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

    public Set<ThriftHeaderParameter> getHeaderParameters()
    {
        return headerParameters;
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

    public boolean isIdempotent()
    {
        return idempotent;
    }

    public List<String> getDocumentation()
    {
        return documentation;
    }

    private ImmutableMap<Short, ThriftType> buildExceptionMap(ThriftCatalog catalog, ThriftMethod thriftMethod)
    {
        boolean mixedStyle = (thriftMethod.exception().length > 0) &&
                stream(method.getAnnotatedExceptionTypes()).anyMatch(type -> type.isAnnotationPresent(ThriftId.class));
        checkArgument(!mixedStyle, "ThriftMethod [%s] uses a mix of @ThriftException and @ThriftId", methodName(method));

        Map<Short, ThriftType> exceptions = new HashMap<>();
        Set<Type> exceptionTypes = new HashSet<>();

        for (ThriftException thriftException : thriftMethod.exception()) {
            checkArgument(!exceptions.containsKey(thriftException.id()), "ThriftMethod [%s] exception list contains multiple values for field ID [%s]", methodName(method), thriftException.id());
            checkArgument(!exceptionTypes.contains(thriftException.type()), "ThriftMethod [%s] exception list contains multiple values for type [%s]", methodName(method), thriftException.type().getSimpleName());
            exceptions.put(thriftException.id(), catalog.getThriftType(thriftException.type()));
            exceptionTypes.add(thriftException.type());
        }

        Class<?>[] allExceptionClasses = method.getExceptionTypes();
        AnnotatedType[] exceptionAnnotations = method.getAnnotatedExceptionTypes();
        for (int i = 0; i < allExceptionClasses.length; i++) {
            Class<?> exception = allExceptionClasses[i];
            ThriftId thriftId = exceptionAnnotations[i].getAnnotation(ThriftId.class);
            if (thriftId != null) {
                checkArgument(!exceptions.containsKey(thriftId.value()), "ThriftMethod [%s] exception list contains multiple values for field ID [%s]", methodName(method), thriftId.value());
                checkArgument(!exceptionTypes.contains(exception), "ThriftMethod [%s] exception list contains multiple values for type [%s]", methodName(method), exception.getSimpleName());
                exceptions.put(thriftId.value(), catalog.getThriftType(exception));
                exceptionTypes.add(exception);
            }
        }

        // the built-in exception types don't need special treatment
        List<Class<?>> exceptionClasses = stream(method.getExceptionTypes())
                .filter(exception -> !exception.isAssignableFrom(TException.class))
                .collect(toList());

        for (Class<?> exceptionClass : exceptionClasses) {
            checkArgument(exceptionClass.isAnnotationPresent(ThriftStruct.class), "ThriftMethod [%s] exception [%s] is not annotated with @ThriftStruct", methodName(method), exceptionClass.getSimpleName());

            if (!exceptionTypes.contains(exceptionClass)) {
                // there is no ordering guarantee for exception types,
                // so we can only infer the id if there is a single custom exception
                checkArgument(exceptionClasses.size() == 1, "ThriftMethod [%s] annotation must declare exception mapping when more than one custom exception is thrown", methodName(method));
                exceptions.put((short) 1, catalog.getThriftType(exceptionClass));
            }
        }

        return ImmutableMap.copyOf(exceptions);
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
                Objects.equals(headerParameters, that.headerParameters) &&
                Objects.equals(method, that.method) &&
                Objects.equals(exceptions, that.exceptions);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, returnType, parameters, headerParameters, method, exceptions, oneway);
    }

    private static String methodName(Method method)
    {
        return method.getDeclaringClass().getName() + "." + method.getName();
    }
}
