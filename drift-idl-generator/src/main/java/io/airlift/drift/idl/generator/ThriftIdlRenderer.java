/*
 * Copyright (C) 2017 Facebook, Inc.
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
package io.airlift.drift.idl.generator;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.airlift.drift.annotations.ThriftField.Requiredness;
import io.airlift.drift.codec.ThriftProtocolType;
import io.airlift.drift.codec.metadata.ThriftEnumMetadata;
import io.airlift.drift.codec.metadata.ThriftFieldMetadata;
import io.airlift.drift.codec.metadata.ThriftMethodMetadata;
import io.airlift.drift.codec.metadata.ThriftServiceMetadata;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;
import io.airlift.drift.codec.metadata.ThriftType;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.collect.Streams.mapWithIndex;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@SuppressFBWarnings("VA_FORMAT_STRING_USES_NEWLINE")
public final class ThriftIdlRenderer
{
    private final ThriftTypeRenderer typeRenderer;

    private ThriftIdlRenderer(ThriftTypeRenderer typeRenderer)
    {
        this.typeRenderer = requireNonNull(typeRenderer, "typeRenderer is null");
    }

    public static String renderThriftIdl(
            Map<String, String> namespaces,
            Set<String> includes,
            List<ThriftType> thriftTypes,
            List<ThriftServiceMetadata> services,
            ThriftTypeRenderer typeRenderer)
    {
        List<ThriftEnumMetadata<?>> enums = thriftTypes.stream()
                .filter(type -> type.getProtocolType() == ThriftProtocolType.ENUM)
                .map(ThriftType::getEnumMetadata)
                .collect(toList());

        List<ThriftStructMetadata> structs = thriftTypes.stream()
                .filter(type -> type.getProtocolType() == ThriftProtocolType.STRUCT)
                .map(ThriftType::getStructMetadata)
                .collect(toList());

        return new ThriftIdlRenderer(typeRenderer)
                .render(namespaces, includes, enums, structs, services);
    }

    private String render(
            Map<String, String> namespaces,
            Set<String> includes,
            List<ThriftEnumMetadata<?>> enums,
            List<ThriftStructMetadata> structs,
            List<ThriftServiceMetadata> services)
    {
        return Joiner.on("\n").skipNulls().join(
                renderNamespaces(namespaces),
                renderIncludes(includes),
                renderEnums(enums),
                renderStructs(structs),
                renderServices(services));
    }

    private static String renderNamespaces(Map<String, String> namespaces)
    {
        if (namespaces.isEmpty()) {
            return null;
        }
        return namespaces.entrySet().stream()
                .map(entry -> format("namespace %s %s", entry.getKey(), entry.getValue()))
                .collect(joining("\n")) + "\n";
    }

    private static String renderIncludes(Set<String> includes)
    {
        if (includes.isEmpty()) {
            return null;
        }
        return includes.stream()
                .map(include -> format("include \"%s\"", include))
                .collect(joining("\n")) + "\n";
    }

    private static String renderEnums(List<ThriftEnumMetadata<?>> enums)
    {
        return emptyToNull(enums.stream()
                .map(ThriftIdlRenderer::renderEnum)
                .collect(joining("\n")));
    }

    private static String renderEnum(ThriftEnumMetadata<?> metadata)
    {
        return documentation(metadata.getDocumentation(), "") +
                format("enum %s {\n%s}\n", metadata.getEnumName(), renderEnumElements(metadata));
    }

    private static <T extends Enum<T>> String renderEnumElements(ThriftEnumMetadata<T> metadata)
    {
        if (metadata.getElementsDocumentation().values().stream().allMatch(Collection::isEmpty) &&
                (metadata.getByEnumConstant() == null)) {
            return "  " + metadata.getElementsDocumentation().keySet().stream()
                    .map(Enum::name)
                    .collect(joining(", ")) + "\n";
        }

        LineSeparator separator = new LineSeparator();
        StringBuilder builder = new StringBuilder();
        for (Entry<T, ImmutableList<String>> entry : metadata.getElementsDocumentation().entrySet()) {
            builder.append(separator.getAndUpdate(!entry.getValue().isEmpty()))
                    .append(documentation(entry.getValue(), "  "))
                    .append("  ").append(entry.getKey().name());
            if (metadata.getByEnumConstant() != null) {
                builder.append(" = ").append(metadata.getByEnumConstant().get(entry.getKey()));
            }
            builder.append(";\n");
        }
        return builder.toString();
    }

    private String renderStructs(List<ThriftStructMetadata> structs)
    {
        return emptyToNull(structs.stream()
                .map(this::renderStruct)
                .collect(joining("\n")));
    }

    private String renderStruct(ThriftStructMetadata struct)
    {
        StringBuilder builder = new StringBuilder()
                .append(documentation(struct.getDocumentation(), ""))
                .append(format("%s %s {\n", structKind(struct), struct.getStructName()));

        LineSeparator separator = new LineSeparator();
        for (ThriftFieldMetadata field : struct.getFields()) {
            if (field.isInternal()) {
                continue;
            }
            builder.append(separator.getAndUpdate(!field.getDocumentation().isEmpty()))
                    .append(documentation(field.getDocumentation(), "  "))
                    .append(format("  %s: %s%s %s;\n",
                            field.getId(),
                            requiredness(field.getRequiredness()),
                            typeName(field.getThriftType()),
                            field.getName()));
        }

        return builder.append("}\n").toString();
    }

    private static String structKind(ThriftStructMetadata struct)
    {
        if (struct.isStruct()) {
            return "struct";
        }
        if (struct.isException()) {
            return "exception";
        }
        if (struct.isUnion()) {
            return "union";
        }
        throw new IllegalArgumentException("Unknown type: " + struct);
    }

    private static String requiredness(Requiredness requiredness)
    {
        switch (requiredness) {
            case REQUIRED:
                return "required ";
            case OPTIONAL:
                return "optional ";
        }
        return "";
    }

    private String renderServices(List<ThriftServiceMetadata> services)
    {
        return emptyToNull(services.stream()
                .map(this::renderService)
                .collect(joining("\n")));
    }

    private String renderService(ThriftServiceMetadata service)
    {
        return documentation(service.getDocumentation(), "") +
                format("service %s {\n", service.getIdlName()) +
                service.getMethods().values().stream()
                        .map(this::renderMethod)
                        .collect(joining("\n")) +
                "}\n";
    }

    private String renderMethod(ThriftMethodMetadata method)
    {
        StringBuilder builder = new StringBuilder()
                .append(documentation(method.getDocumentation(), "  "));

        String methodStart = format("  %s%s %s(",
                method.getOneway() ? "oneway " : "",
                typeName(method.getReturnType()),
                method.getName());

        List<String> parameters = method.getParameters().stream()
                .map(parameter -> format("%s: %s %s",
                        parameter.getId(),
                        typeName(parameter.getThriftType()),
                        parameter.getName()))
                .collect(toList());

        builder.append(renderParameters(methodStart, parameters));

        if (!method.getExceptions().isEmpty()) {
            List<String> exceptions = mapWithIndex(method.getExceptions().entrySet().stream(),
                    (entry, index) -> format("%s: %s ex%s",
                            entry.getKey(),
                            typeName(entry.getValue().getThriftType()),
                            index + 1))
                    .collect(toList());
            builder.append("\n")
                    .append(renderParameters("    throws (", exceptions));
        }

        return builder.append(";\n").toString();
    }

    private static String renderParameters(String start, List<String> parameters)
    {
        String joined = Joiner.on(", ").join(parameters) + ")";
        if ((start.length() + joined.length()) <= 80) {
            return start + joined;
        }
        return start + "\n      " + Joiner.on(",\n      ").join(parameters) + ")";
    }

    private String typeName(ThriftType type)
    {
        return typeRenderer.toString(type);
    }

    private static String documentation(List<String> documentation, String indent)
    {
        if (documentation.isEmpty()) {
            return "";
        }
        StringBuilder builder = new StringBuilder()
                .append(indent).append("/**\n");
        for (String line : documentation) {
            builder.append(indent).append(" *");
            if (!line.isEmpty()) {
                builder.append(" ").append(line);
            }
            builder.append("\n");
        }
        return builder.append(indent).append(" */\n").toString();
    }

    private static class LineSeparator
    {
        private boolean first = true;
        private boolean wasDocumented;

        public String getAndUpdate(boolean documented)
        {
            boolean needed = !first && (wasDocumented || documented);
            first = false;
            wasDocumented = documented;
            return needed ? "\n" : "";
        }
    }
}
