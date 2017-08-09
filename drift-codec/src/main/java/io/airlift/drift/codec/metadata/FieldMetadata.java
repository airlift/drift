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
package io.airlift.drift.codec.metadata;

import com.google.common.collect.ImmutableMap;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftIdlAnnotation;

import javax.annotation.Nullable;

import java.lang.reflect.Type;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness;
import static io.airlift.drift.annotations.ThriftIdlAnnotation.RECURSIVE_REFERENCE_ANNOTATION_KEY;
import static java.util.Objects.requireNonNull;

abstract class FieldMetadata
{
    private Short id;
    private Boolean isLegacyId;
    private Boolean isRecursiveReference;
    private String name;
    private Requiredness requiredness;
    private Map<String, String> idlAnnotations;
    private final FieldKind type;

    protected FieldMetadata(ThriftField annotation, FieldKind type)
    {
        this.type = type;

        switch (type) {
            case THRIFT_FIELD:
                if (annotation != null) {
                    if (annotation.value() != Short.MIN_VALUE) {
                        id = annotation.value();
                    }
                    isLegacyId = annotation.isLegacyId();
                    if (!annotation.name().isEmpty()) {
                        name = annotation.name();
                    }
                    requiredness = requireNonNull(annotation.requiredness());

                    ImmutableMap.Builder<String, String> annotationMapBuilder = ImmutableMap.builder();
                    for (ThriftIdlAnnotation idlAnnotation : annotation.idlAnnotations()) {
                        annotationMapBuilder.put(idlAnnotation.key(), idlAnnotation.value());
                    }
                    idlAnnotations = annotationMapBuilder.build();

                    if (annotation.isRecursive() != ThriftField.Recursiveness.UNSPECIFIED) {
                        switch (annotation.isRecursive()) {
                            case TRUE:
                                isRecursiveReference = true;
                                break;
                            case FALSE:
                                isRecursiveReference = false;
                                break;
                            default:
                                throw new IllegalStateException("Unexpected get for isRecursive field");
                        }
                    }
                    else if (idlAnnotations.containsKey(RECURSIVE_REFERENCE_ANNOTATION_KEY)) {
                        isRecursiveReference = "true".equalsIgnoreCase(idlAnnotations.getOrDefault(RECURSIVE_REFERENCE_ANNOTATION_KEY, "false"));
                    }
                }
                break;
            case THRIFT_UNION_ID:
                checkArgument(annotation == null, "ThriftStruct annotation not allowed in union");
                id = Short.MIN_VALUE;
                isLegacyId = true; // preserve `negative field ID <=> isLegacyId`
                name = "_union_id";
                break;
            default:
                throw new IllegalArgumentException("Encountered field metadata type " + type);
        }
    }

    public Short getId()
    {
        return id;
    }

    public void setId(short id)
    {
        this.id = id;
    }

    @Nullable
    public Boolean isLegacyId()
    {
        return isLegacyId;
    }

    public void setIsLegacyId(Boolean isLegacyId)
    {
        this.isLegacyId = isLegacyId;
    }

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public Map<String, String> getIdlAnnotations()
    {
        return idlAnnotations;
    }

    public void setIdlAnnotations(Map<String, String> idlAnnotations)
    {
        this.idlAnnotations = idlAnnotations;
    }

    public FieldKind getType()
    {
        return type;
    }

    public abstract Type getJavaType();

    public abstract String extractName();

    /**
     * Returns the `isLegacyId` setting from a FieldMetadata, if present.
     */
    public Optional<Boolean> getThriftFieldIsLegacyId()
    {
        Boolean value = isLegacyId();
        if (getId() == null || getId() == Short.MIN_VALUE) {
            if ((value != null) && !value) {
                return Optional.empty();
            }
        }

        return Optional.ofNullable(value);
    }

    public String getOrExtractThriftFieldName()
    {
        String name = getName();
        if (name == null) {
            name = extractName();
        }
        if (name == null) {
            throw new IllegalStateException("name is null");
        }
        return name;
    }

    public Requiredness getRequiredness()
    {
        return requiredness;
    }

    public void setRequiredness(Requiredness requiredness)
    {
        this.requiredness = requiredness;
    }

    @Nullable
    public Boolean isRecursiveReference()
    {
        return isRecursiveReference;
    }

    public void setIsRecursiveReference(Boolean isRecursiveReference)
    {
        this.isRecursiveReference = isRecursiveReference;
    }
}
