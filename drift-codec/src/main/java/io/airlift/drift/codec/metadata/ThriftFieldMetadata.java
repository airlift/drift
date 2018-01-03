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

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness;
import static io.airlift.drift.annotations.ThriftIdlAnnotation.RECURSIVE_REFERENCE_ANNOTATION_KEY;
import static java.util.Objects.requireNonNull;

/**
 * ThriftFieldMetadata defines a single thrift field including the value extraction and injection
 * points.
 */
@Immutable
public class ThriftFieldMetadata
{
    private final short id;
    private final ThriftTypeReference thriftTypeReference;
    private final String name;
    private final FieldKind fieldKind;
    private final List<ThriftInjection> injections;
    private final Map<String, String> idlAnnotations;
    private final Optional<ThriftConstructorInjection> constructorInjection;
    private final Optional<ThriftMethodInjection> methodInjection;
    private final Optional<ThriftExtraction> extraction;
    private final Optional<TypeCoercion> coercion;
    private final ImmutableList<String> documentation;
    private final boolean isRecursiveReference;
    private final Requiredness requiredness;

    public ThriftFieldMetadata(
            short id,
            boolean isLegacyId,
            boolean isRecursiveReference,
            Requiredness requiredness,
            Map<String, String> idlAnnotations,
            ThriftTypeReference thriftTypeReference,
            String name,
            FieldKind fieldKind,
            List<ThriftInjection> injections,
            Optional<ThriftConstructorInjection> constructorInjection,
            Optional<ThriftMethodInjection> methodInjection,
            Optional<ThriftExtraction> extraction,
            Optional<TypeCoercion> coercion)
    {
        this.isRecursiveReference = isRecursiveReference;
        this.requiredness = requiredness;
        this.thriftTypeReference = requireNonNull(thriftTypeReference, "thriftType is null");
        this.fieldKind = requireNonNull(fieldKind, "type is null");
        this.name = requireNonNull(name, "name is null");
        this.injections = ImmutableList.copyOf(requireNonNull(injections, "injections is null"));
        this.constructorInjection = requireNonNull(constructorInjection, "constructorInjection is null");
        this.methodInjection = requireNonNull(methodInjection, "methodInjection is null");

        this.extraction = requireNonNull(extraction, "extraction is null");
        this.coercion = requireNonNull(coercion, "coercion is null");

        switch (fieldKind) {
            case THRIFT_FIELD:
                if (isLegacyId) {
                    checkArgument(id < 0, "isLegacyId should only be specified on fields with negative IDs");
                }
                else {
                    checkArgument(id >= 0, "isLegacyId must be specified on fields with negative IDs");
                }
                break;
            case THRIFT_UNION_ID:
                checkArgument(isLegacyId, "isLegacyId should be implicitly set on ThriftUnionId fields");
                checkArgument(id == Short.MIN_VALUE, "thrift union id must be Short.MIN_VALUE");
                break;
        }

        checkArgument(!injections.isEmpty()
                || extraction.isPresent()
                || constructorInjection.isPresent()
                || methodInjection.isPresent(), "A thrift field must have an injection or extraction point");

        this.id = id;

        if (extraction.isPresent()) {
            if (extraction.get() instanceof ThriftFieldExtractor) {
                ThriftFieldExtractor e = (ThriftFieldExtractor) extraction.get();
                this.documentation = ThriftCatalog.getThriftDocumentation(e.getField());
            }
            else if (extraction.get() instanceof ThriftMethodExtractor) {
                ThriftMethodExtractor e = (ThriftMethodExtractor) extraction.get();
                this.documentation = ThriftCatalog.getThriftDocumentation(e.getMethod());
            }
            else {
                this.documentation = ImmutableList.of();
            }
        }
        else {
            // no extraction = no documentation
            this.documentation = ImmutableList.of();
        }

        this.idlAnnotations = idlAnnotations;
    }

    public short getId()
    {
        return id;
    }

    public ThriftType getThriftType()
    {
        return thriftTypeReference.get();
    }

    public Requiredness getRequiredness()
    {
        return requiredness;
    }

    public String getName()
    {
        return name;
    }

    public FieldKind getType()
    {
        return fieldKind;
    }

    public Map<String, String> getIdlAnnotations()
    {
        ImmutableMap.Builder<String, String> annotationsBuilder = ImmutableMap.builder();
        annotationsBuilder.putAll(idlAnnotations);

        if (isRecursiveReference()) {
            annotationsBuilder.put(RECURSIVE_REFERENCE_ANNOTATION_KEY, "true");
        }

        return annotationsBuilder.build();
    }

    public boolean isTypeReferenceRecursive()
    {
        return thriftTypeReference.isRecursive();
    }

    public boolean isRecursiveReference()
    {
        return isRecursiveReference;
    }

    public boolean isInternal()
    {
        switch (getType()) {
            // These are normal thrift fields (i.e. they should be emitted by the drift2thrift generator)
            case THRIFT_FIELD:
                return false;

            // Other fields types are used internally in drift, but do not make up part of the external
            // thrift interface
            default:
                return true;
        }
    }

    public boolean isReadOnly()
    {
        return injections.isEmpty() && !constructorInjection.isPresent() && !methodInjection.isPresent();
    }

    public boolean isWriteOnly()
    {
        return !extraction.isPresent();
    }

    public List<ThriftInjection> getInjections()
    {
        return injections;
    }

    public Optional<ThriftConstructorInjection> getConstructorInjection()
    {
        return constructorInjection;
    }

    public Optional<ThriftMethodInjection> getMethodInjection()
    {
        return methodInjection;
    }

    public Optional<ThriftExtraction> getExtraction()
    {
        return extraction;
    }

    public Optional<TypeCoercion> getCoercion()
    {
        return coercion;
    }

    public ImmutableList<String> getDocumentation()
    {
        return documentation;
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
        ThriftFieldMetadata that = (ThriftFieldMetadata) o;
        return id == that.id &&
                Objects.equals(thriftTypeReference, that.thriftTypeReference) &&
                Objects.equals(name, that.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, thriftTypeReference, name);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("thriftTypeReference", thriftTypeReference)
                .add("name", name)
                .add("fieldKind", fieldKind)
                .add("injections", injections)
                .add("constructorInjection", constructorInjection)
                .add("methodInjection", methodInjection)
                .add("extraction", extraction)
                .add("coercion", coercion)
                .toString();
    }
}
