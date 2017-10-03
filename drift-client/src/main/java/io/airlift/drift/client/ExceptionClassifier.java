/*
 * Copyright (C) 2013 Facebook, Inc.
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

import com.google.common.collect.ImmutableList;

import java.util.List;

import static io.airlift.drift.client.ExceptionClassification.NORMAL_EXCEPTION;
import static io.airlift.drift.client.ExceptionClassification.mergeExceptionClassifications;
import static java.util.Objects.requireNonNull;

public interface ExceptionClassifier
{
    ExceptionClassifier NORMAL_RESULT = throwable -> NORMAL_EXCEPTION;

    static ExceptionClassifier mergeExceptionClassifiers(Iterable<? extends ExceptionClassifier> classifiers)
    {
        List<ExceptionClassifier> exceptionClassifiers = ImmutableList.copyOf(requireNonNull(classifiers, "classifiers is null"));
        return throwable -> exceptionClassifiers.stream()
                .map(classifier -> classifier.classifyException(throwable))
                .collect(mergeExceptionClassifications());
    }

    ExceptionClassification classifyException(Throwable throwable);
}
