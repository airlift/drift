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
package io.airlift.drift.javadoc;

import com.squareup.javapoet.AnnotationSpec;
import com.squareup.javapoet.FieldSpec;
import com.squareup.javapoet.JavaFile;
import com.squareup.javapoet.MethodSpec;
import com.squareup.javapoet.TypeSpec;
import io.airlift.drift.annotations.ThriftDocumentation;
import io.airlift.drift.annotations.ThriftOrder;

import javax.annotation.processing.AbstractProcessor;
import javax.annotation.processing.Filer;
import javax.annotation.processing.RoundEnvironment;
import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeMirror;
import javax.lang.model.util.Elements;
import javax.tools.Diagnostic;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.drift.javadoc.ThriftAnnotations.META_SUFFIX;
import static io.airlift.drift.javadoc.ThriftAnnotations.THRIFT_FIELD;
import static io.airlift.drift.javadoc.ThriftAnnotations.THRIFT_METHOD;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static javax.lang.model.element.ElementKind.ENUM_CONSTANT;
import static javax.tools.Diagnostic.Kind.ERROR;
import static javax.tools.Diagnostic.Kind.NOTE;
import static javax.tools.Diagnostic.Kind.WARNING;

@SupportedAnnotationTypes({
        ThriftAnnotations.THRIFT_ENUM,
        ThriftAnnotations.THRIFT_SERVICE,
        ThriftAnnotations.THRIFT_STRUCT})
@SupportedSourceVersion(SourceVersion.RELEASE_8)
public class JavadocProcessor
        extends AbstractProcessor
{
    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment round)
    {
        for (TypeElement annotation : annotations) {
            for (Element element : round.getElementsAnnotatedWith(annotation)) {
                if (element instanceof TypeElement) {
                    log(NOTE, "Extracting Javadoc metadata for " + element);
                    extract((TypeElement) element);
                }
            }
        }
        return false;
    }

    private void extract(TypeElement typeElement)
    {
        if (typeElement.getQualifiedName().toString().endsWith(META_SUFFIX)) {
            return;
        }

        switch (typeElement.getKind()) {
            case CLASS:
            case INTERFACE:
            case ENUM:
                break;
            default:
                log(WARNING, format("Non-class was annotated: %s %s", typeElement.getKind(), typeElement));
                return;
        }

        List<String> serviceDocumentation = getComment(typeElement);

        String packageName = elements().getPackageOf(typeElement).getQualifiedName().toString();
        String className = getClassName(typeElement);

        TypeSpec.Builder typeSpec = TypeSpec.classBuilder(className + META_SUFFIX)
                .addAnnotation(documentationAnnotation(serviceDocumentation));

        // offset auto-generated order numbers so they don't collide with hand-written ones
        AtomicInteger nextOrder = new AtomicInteger(10000);

        for (Element element : elements().getAllMembers(typeElement)) {
            String name = element.getSimpleName().toString();
            List<String> comment = getComment(element);
            if ((element instanceof ExecutableElement) &&
                    (isAnnotatedWith(element, THRIFT_METHOD) || isAnnotatedWith(element, THRIFT_FIELD))) {
                typeSpec.addMethod(MethodSpec.methodBuilder(name)
                        .addAnnotation(documentationAnnotation(comment))
                        .addAnnotation(orderAnnotation(nextOrder.getAndIncrement()))
                        .build());
            }
            else if (((element instanceof VariableElement) && isAnnotatedWith(element, THRIFT_FIELD)) ||
                    (element.getKind() == ENUM_CONSTANT)) {
                typeSpec.addField(FieldSpec.builder(int.class, name)
                        .addAnnotation(documentationAnnotation(comment))
                        .addAnnotation(orderAnnotation(nextOrder.getAndIncrement()))
                        .build());
            }
        }

        JavaFile javaFile = JavaFile.builder(packageName, typeSpec.build()).build();

        String name = typeElement.getQualifiedName() + META_SUFFIX;

        try (Writer writer = filer().createSourceFile(name, typeElement).openWriter()) {
            javaFile.writeTo(writer);
        }
        catch (IOException e) {
            log(ERROR, format("Failed to create %s%s file", typeElement, META_SUFFIX));
        }
    }

    private static AnnotationSpec documentationAnnotation(List<String> lines)
    {
        AnnotationSpec.Builder builder = AnnotationSpec.builder(ThriftDocumentation.class);
        for (String line : lines) {
            builder.addMember("value", "$S", line);
        }
        return builder.build();
    }

    private static AnnotationSpec orderAnnotation(int value)
    {
        return AnnotationSpec.builder(ThriftOrder.class)
                .addMember("value", "$L", value)
                .build();
    }

    private String getClassName(TypeElement typeElement)
    {
        // handle nested classes
        String binaryName = elements().getBinaryName(typeElement).toString();
        return binaryName.substring(binaryName.lastIndexOf('.') + 1);
    }

    private static boolean isAnnotatedWith(Element element, String annotation)
    {
        return element.getAnnotationMirrors().stream()
                .map(AnnotationMirror::getAnnotationType)
                .map(TypeMirror::toString)
                .anyMatch(annotation::equals);
    }

    private List<String> getComment(Element element)
    {
        String comment = elements().getDocComment(element);
        if (comment == null) {
            return emptyList();
        }

        return Arrays.stream(comment.split("\n"))
                .map(String::trim)
                .collect(toList());
    }

    private Elements elements()
    {
        return processingEnv.getElementUtils();
    }

    private Filer filer()
    {
        return processingEnv.getFiler();
    }

    private void log(Diagnostic.Kind kind, String message)
    {
        processingEnv.getMessager().printMessage(kind, message);
    }
}
