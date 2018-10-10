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
package io.airlift.drift.codec;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import io.airlift.drift.codec.generics.ConcreteDerivedFromGeneric;
import io.airlift.drift.codec.generics.ConcreteDerivedFromGenericBean;
import io.airlift.drift.codec.generics.ConcreteThriftStructDerivedFromGenericField;
import io.airlift.drift.codec.generics.GenericThriftStruct;
import io.airlift.drift.codec.generics.GenericThriftStructBean;
import io.airlift.drift.codec.generics.GenericThriftStructField;
import io.airlift.drift.codec.generics.GenericThriftStructFromBuilder;
import io.airlift.drift.codec.internal.EnumThriftCodec;
import io.airlift.drift.codec.internal.coercion.DefaultJavaCoercions;
import io.airlift.drift.codec.metadata.ThriftCatalog;
import io.airlift.drift.codec.metadata.ThriftStructMetadata;
import io.airlift.drift.codec.metadata.ThriftType;
import io.airlift.drift.codec.recursion.CoRecursive;
import io.airlift.drift.codec.recursion.CoRecursiveHelper;
import io.airlift.drift.codec.recursion.CoRecursiveTree;
import io.airlift.drift.codec.recursion.CoRecursiveTreeHelper;
import io.airlift.drift.codec.recursion.RecursiveDefaultUnion;
import io.airlift.drift.codec.recursion.RecursiveUnion;
import io.airlift.drift.codec.recursion.ViaListElementType;
import io.airlift.drift.codec.recursion.ViaMapKeyAndValueTypes;
import io.airlift.drift.codec.recursion.ViaNestedListElementType;
import io.airlift.drift.codec.recursion.WithDriftRecursiveAnnotation;
import io.airlift.drift.codec.recursion.WithIdlRecursiveAnnotation;
import io.airlift.drift.codec.recursion.WithoutRecursiveAnnotation;
import io.airlift.drift.protocol.TBinaryProtocol;
import io.airlift.drift.protocol.TCompactProtocol;
import io.airlift.drift.protocol.TFacebookCompactProtocol;
import io.airlift.drift.protocol.TMemoryBuffer;
import io.airlift.drift.protocol.TProtocol;
import io.airlift.drift.protocol.TTransport;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public abstract class AbstractThriftCodecManagerTest
{
    private ThriftCodecManager readCodecManager;
    private ThriftCodecManager writeCodecManager;

    protected abstract ThriftCodecManager createReadCodecManager();

    protected abstract ThriftCodecManager createWriteCodecManager();

    @BeforeMethod
    protected void setUp()
    {
        readCodecManager = createReadCodecManager();
        writeCodecManager = createWriteCodecManager();
        readCodecManager.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
        writeCodecManager.getCatalog().addDefaultCoercions(DefaultJavaCoercions.class);
    }

    @Test
    public void testUnionFieldsManual()
            throws Exception
    {
        ThriftCatalog catalog = new ThriftCatalog();
        ThriftType unionFieldType = catalog.getThriftType(UnionField.class);
        ThriftType fruitType = catalog.getThriftType(Fruit.class);
        ThriftCodec<Fruit> fruitCodec = new EnumThriftCodec<>(fruitType);
        UnionFieldThriftCodec unionFieldCodec = new UnionFieldThriftCodec(unionFieldType, fruitCodec);

        UnionField unionField = new UnionField();
        unionField.id = 1;
        unionField.stringValue = "Hello, World";

        testRoundTripSerialize(unionFieldCodec, unionFieldCodec, unionField);

        unionField = new UnionField();
        unionField.id = 2;
        unionField.longValue = 4815162342L;

        testRoundTripSerialize(unionFieldCodec, unionFieldCodec, unionField);

        unionField = new UnionField();
        unionField.id = 3;
        unionField.fruitValue = Fruit.APPLE; // The best fruit!

        testRoundTripSerialize(unionFieldCodec, unionFieldCodec, unionField);
    }

    @Test
    public void testUnionFields()
            throws Exception
    {
        UnionField unionField = new UnionField();
        unionField.id = 1;
        unionField.stringValue = "Hello, World";

        testRoundTripSerialize(unionField);

        unionField = new UnionField();
        unionField.id = 2;
        unionField.longValue = 4815162342L;

        testRoundTripSerialize(unionField);

        unionField = new UnionField();
        unionField.id = 3;
        unionField.fruitValue = Fruit.APPLE; // The best fruit!

        testRoundTripSerialize(unionField);
    }

    @Test
    public void testUnionBean()
            throws Exception
    {
        UnionBean unionBean = new UnionBean();
        unionBean.setStringValue("Hello, World");
        testRoundTripSerialize(unionBean);

        unionBean = new UnionBean();
        unionBean.setLongValue(4815162342L);
        testRoundTripSerialize(unionBean);

        unionBean = new UnionBean();
        unionBean.setFruitValue(Fruit.CHERRY);
        testRoundTripSerialize(unionBean);
    }

    @Test
    public void testUnionConstructor()
            throws Exception
    {
        UnionConstructor unionConstructor = new UnionConstructor("Hello, World");
        testRoundTripSerialize(unionConstructor);

        unionConstructor = new UnionConstructor(4815162342L);
        testRoundTripSerialize(unionConstructor);

        unionConstructor = new UnionConstructor(Fruit.APPLE);
        testRoundTripSerialize(unionConstructor);

        unionConstructor = new UnionConstructor();
        testRoundTripSerialize(unionConstructor);
    }

    @Test
    public void testUnionConstructorDuplicateTypes()
            throws Exception
    {
        UnionConstructorDuplicateTypes unionConstructor = new UnionConstructorDuplicateTypes();
        unionConstructor.setFirstIntValue(1);
        testRoundTripSerialize(unionConstructor);

        unionConstructor = new UnionConstructorDuplicateTypes();
        unionConstructor.setSecondIntValue(2);
        testRoundTripSerialize(unionConstructor);
    }

    @Test
    public void testStructFieldsManual()
            throws Exception
    {
        ThriftCatalog catalog = new ThriftCatalog();
        ThriftType bonkFieldType = catalog.getThriftType(BonkField.class);
        BonkFieldThriftCodec bonkFieldCodec = new BonkFieldThriftCodec(bonkFieldType);

        BonkField bonkField = new BonkField("message", 42);
        testRoundTripSerialize(bonkFieldCodec, bonkFieldCodec, bonkField);
    }

    @Test
    public void testStructFields()
            throws Exception
    {
        BonkField bonkField = new BonkField("message", 42);
        testRoundTripSerialize(bonkField);
    }

    @Test
    public void testStructBean()
            throws Exception
    {
        BonkBean bonkBean = new BonkBean("message", 42);
        testRoundTripSerialize(bonkBean);
    }

    @Test
    public void testStructMethod()
            throws Exception
    {
        BonkMethod bonkMethod = new BonkMethod("message", 42);
        testRoundTripSerialize(bonkMethod);
    }

    @Test
    public void testStructConstructor()
            throws Exception
    {
        BonkConstructor bonkConstructor = new BonkConstructor("message", 42);
        testRoundTripSerialize(bonkConstructor);
    }

    @Test
    public void testMatchByJavaNameWithThriftNameOverride()
            throws Exception
    {
        ThriftCatalog catalog = readCodecManager.getCatalog();
        ThriftType thriftType = catalog.getThriftType(BonkConstructorNameOverride.class);
        ThriftStructMetadata structMetadata = thriftType.getStructMetadata();
        assertEquals(structMetadata.getField(1).getName(), "myMessage");
        assertEquals(structMetadata.getField(2).getName(), "myType");

        BonkConstructorNameOverride bonk = new BonkConstructorNameOverride("message", 42);
        testRoundTripSerialize(bonk);
    }

    @Test
    public void testBuilder()
            throws Exception
    {
        BonkBuilder bonkBuilder = new BonkBuilder("message", 42);
        testRoundTripSerialize(bonkBuilder);
    }

    @Test
    public void testArraysManual()
            throws Exception
    {
        ThriftCatalog catalog = new ThriftCatalog();
        ThriftType thriftType = catalog.getThriftType(ArrayField.class);
        ArrayFieldThriftCodec arrayFieldCodec = new ArrayFieldThriftCodec(thriftType);

        // manual codec does not implement the Map fields
        ArrayField arrayField = new ArrayField(
                new boolean[] {true, false, false, true},
                new short[] {0, 1, 2, 3},
                new int[] {10, 11, 12, 13},
                new long[] {20, Long.MAX_VALUE, Long.MIN_VALUE},
                new double[] {3.0, Double.MAX_VALUE, Double.MIN_VALUE},
                "hello".getBytes(UTF_8));
        testRoundTripSerialize(arrayFieldCodec, arrayFieldCodec, arrayField);
    }

    @Test
    public void testArrays()
            throws Exception
    {
        ArrayField arrayField = new ArrayField(
                new boolean[] {true, false, false, true},
                new short[] {0, 1, 2, 3},
                new int[] {10, 11, 12, 13},
                new long[] {20, Long.MAX_VALUE, Long.MIN_VALUE},
                new double[] {3.0, Double.MAX_VALUE, Double.MIN_VALUE},
                "hello".getBytes(UTF_8),
                ImmutableMap.of((short) 1, new boolean[] {false, false}, (short) 2, new boolean[] {true, true}),
                ImmutableMap.of((short) 1, new short[] {10, 11, 12, 13}, (short) 2, new short[] {15, 16, 17, 18}),
                ImmutableMap.of((short) 1, new int[] {20, 21, 22, 23}, (short) 2, new int[] {25, 26, 27, 28}),
                ImmutableMap.of((short) 1, new long[] {30, 31, 32, 33}, (short) 2, new long[] {35, 36, 37, 38}),
                ImmutableMap.of((short) 1, new double[] {40, 41, 42, 43}, (short) 2, new double[] {45, 46, 47, 48}));

        testRoundTripSerialize(arrayField, TCompactProtocol::new);
    }

    @Test
    public void testAllOptionalField()
            throws Exception
    {
        OptionalField optionalField = new OptionalField();
        optionalField.aBooleanOptional = Optional.of(true);
        optionalField.aByteOptional = Optional.of(Byte.MIN_VALUE);
        optionalField.aShortOptional = Optional.of(Short.MIN_VALUE);
        optionalField.aIntegerOptional = Optional.of(Integer.MIN_VALUE);
        optionalField.aLongOptional = Optional.of(Long.MIN_VALUE);
        optionalField.aDoubleOptional = Optional.of(-42.1d);
        optionalField.aStringOptional = Optional.of("a");
        optionalField.aStructOptional = Optional.of(new BonkField("message", 42));
        optionalField.aEnumOptional = Optional.of(Fruit.BANANA);

        optionalField.aOptionalDouble = OptionalDouble.of(87.6d);
        optionalField.aOptionalInt = OptionalInt.of(Integer.MAX_VALUE - 10);
        optionalField.aOptionalLong = OptionalLong.of(Long.MAX_VALUE - 20);

        optionalField.aCustomEnumOptional = Optional.of(Letter.C);
        optionalField.aListBooleanOptional = Optional.of(ImmutableList.of(true));
        optionalField.aListByteOptional = Optional.of(ImmutableList.of(Byte.MAX_VALUE));
        optionalField.aListShortOptional = Optional.of(ImmutableList.of(Short.MAX_VALUE));
        optionalField.aListIntegerOptional = Optional.of(ImmutableList.of(Integer.MAX_VALUE));
        optionalField.aListLongOptional = Optional.of(ImmutableList.of(Long.MAX_VALUE));
        optionalField.aListDoubleOptional = Optional.of(ImmutableList.of(-42.1d));
        optionalField.aListStringOptional = Optional.of(ImmutableList.of("a"));
        optionalField.aListStructOptional = Optional.of(ImmutableList.of(new BonkField("message", 42)));
        optionalField.aListEnumOptional = Optional.of(ImmutableList.of(Fruit.BANANA));
        optionalField.aListCustomEnumOptional = Optional.of(ImmutableList.of(Letter.C));

        testRoundTripSerialize(optionalField);
    }

    @Test
    public void testAllOptionalFieldEmpty()
            throws Exception
    {
        testRoundTripSerialize(new OptionalField());
    }

    @Test
    public void testAllOptionalStruct()
            throws Exception
    {
        OptionalStruct optionalStruct = new OptionalStruct(
                Optional.of(true),
                Optional.of(Byte.MIN_VALUE),
                Optional.of(Short.MIN_VALUE),
                Optional.of(Integer.MIN_VALUE),
                Optional.of(Long.MIN_VALUE),
                Optional.of(-42.1d),
                Optional.of("a"),
                Optional.of(new BonkField("message", 42)),
                Optional.of(Fruit.BANANA),
                Optional.of(Letter.C),

                OptionalDouble.of(87.6d),
                OptionalInt.of(Integer.MAX_VALUE - 10),
                OptionalLong.of(Long.MAX_VALUE - 20),

                Optional.of(ImmutableList.of(true)),
                Optional.of(ImmutableList.of(Byte.MAX_VALUE)),
                Optional.of(ImmutableList.of(Short.MAX_VALUE)),
                Optional.of(ImmutableList.of(Integer.MAX_VALUE)),
                Optional.of(ImmutableList.of(Long.MAX_VALUE)),
                Optional.of(ImmutableList.of(-42.1d)),
                Optional.of(ImmutableList.of("a")),
                Optional.of(ImmutableList.of(new BonkField("message", 42))),
                Optional.of(ImmutableList.of(Fruit.BANANA)),
                Optional.of(ImmutableList.of(Letter.C)));

        testRoundTripSerialize(optionalStruct);
    }

    @Test
    public void testAllOptionalStructEmpty()
            throws Exception
    {
        testRoundTripSerialize(new OptionalStruct());
    }

    @Test
    public void testOneOfEverythingField()
            throws Exception
    {
        testRoundTripSerialize(createOneOfEverything());
    }

    @Test
    public void testOneOfEverythingFieldManual()
            throws Exception
    {
        ThriftCatalog catalog = readCodecManager.getCatalog();
        ThriftType bonkFieldType = catalog.getThriftType(BonkField.class);
        ThriftType unionFieldType = catalog.getThriftType(UnionField.class);
        ThriftType fruitType = catalog.getThriftType(Fruit.class);

        ThriftCodec<Fruit> fruitCodec = new EnumThriftCodec<>(fruitType);
        BonkFieldThriftCodec bonkFieldCodec = new BonkFieldThriftCodec(bonkFieldType);
        UnionFieldThriftCodec unionFieldCodec = new UnionFieldThriftCodec(unionFieldType, fruitCodec);

        ThriftType oneOfEverythingType = catalog.getThriftType(OneOfEverything.class);

        OneOfEverythingThriftCodec codec = new OneOfEverythingThriftCodec(
                oneOfEverythingType,
                bonkFieldCodec,
                unionFieldCodec,
                fruitCodec);

        // manual codec only support some fields
        OneOfEverything one = new OneOfEverything();
        one.aBoolean = true;
        one.aByte = 11;
        one.aShort = 22;
        one.aInt = 33;
        one.aLong = 44;
        one.aDouble = 55;
        one.aString = "message";
        one.aEnum = Fruit.CHERRY;
        one.aStruct = new BonkField("struct", 66);

        testRoundTripSerialize(codec, codec, one);
    }

    @Test
    public void testOneOfEverythingFieldEmpty()
            throws Exception
    {
        testRoundTripSerialize(new OneOfEverything());
    }

    @Test
    public void testDefaultCoercion()
            throws Exception
    {
        CoercionBean coercion = new CoercionBean(
                true,
                (byte) 1,
                (short) 2,
                3,
                4L,
                5.5f,
                6.6d,
                7.7f,
                ImmutableList.of(1.1f, 2.2f, 3.3f));

        testRoundTripSerialize(coercion);
    }

    @Test
    public void testIsSetBean()
            throws Exception
    {
        IsSetBean full = IsSetBean.createFull();
        assertAllFieldsSet(full, false);
        // manually set full bean
        full.field = ByteBuffer.wrap("full".getBytes(UTF_8));
        testRoundTripSerialize(full, result -> {
            assertAllFieldsSet(result, true);
        });

        IsSetBean empty = IsSetBean.createEmpty();
        assertAllFieldsSet(empty, false);
        testRoundTripSerialize(empty, result -> {
            assertAllFieldsSet(result, false);
        });
    }

    @Test
    public void testBeanGeneric()
            throws Exception
    {
        GenericThriftStructBean<String> bean = new GenericThriftStructBean<>();
        bean.setGenericProperty("genericValue");

        testRoundTripSerialize(new TypeToken<GenericThriftStructBean<String>>() {}, bean);
    }

    @Test
    public void testBeanDerivedFromGeneric()
            throws Exception
    {
        ConcreteDerivedFromGenericBean bean = new ConcreteDerivedFromGenericBean();
        bean.setGenericProperty("generic");
        bean.setConcreteField("concrete");

        testRoundTripSerialize(bean);
    }

    @Test
    public void testImmutableGeneric()
            throws Exception
    {
        GenericThriftStruct<Double> immutable = new GenericThriftStruct<>(Math.PI);

        testRoundTripSerialize(new TypeToken<GenericThriftStruct<Double>>() {}, immutable);
    }

    @Test
    public void testImmutableDerivedFromGeneric()
            throws Exception
    {
        ConcreteDerivedFromGeneric immutable = new ConcreteDerivedFromGeneric(Math.E, Math.PI);

        testRoundTripSerialize(immutable);
    }

    @Test
    public void testGenericFromBuilder()
            throws Exception
    {
        GenericThriftStructFromBuilder<Integer, Double> builderObject =
                new GenericThriftStructFromBuilder.Builder<Integer, Double>()
                        .setFirstGenericProperty(12345)
                        .setSecondGenericProperty(1.2345)
                        .build();

        testRoundTripSerialize(new TypeToken<GenericThriftStructFromBuilder<Integer, Double>>() {}, builderObject);
    }

    @Test
    public void testFieldGeneric()
            throws Exception
    {
        GenericThriftStructField<Integer> fieldObject = new GenericThriftStructField<>();
        fieldObject.genericField = 5757;

        testRoundTripSerialize(new TypeToken<GenericThriftStructField<Integer>>() {}, fieldObject);
    }

    @Test
    public void testFieldDerivedFromGeneric()
            throws Exception
    {
        ConcreteThriftStructDerivedFromGenericField fieldObject = new ConcreteThriftStructDerivedFromGenericField();
        fieldObject.genericField = "genericValue";
        fieldObject.concreteField = "concreteValue";

        testRoundTripSerialize(fieldObject);
    }

    @Test
    public void testRecursiveStructWithDriftAnnotation()
            throws Exception
    {
        WithDriftRecursiveAnnotation recursiveObject = new WithDriftRecursiveAnnotation();
        recursiveObject.data = "parent";
        recursiveObject.child = new WithDriftRecursiveAnnotation();
        recursiveObject.child.data = "child";
        recursiveObject.child.child = new WithDriftRecursiveAnnotation();
        recursiveObject.child.child.data = "grandchild";
        testRoundTripSerialize(recursiveObject);
    }

    @Test
    public void testRecursiveStructWithIdlAnnotation()
            throws Exception
    {
        WithIdlRecursiveAnnotation recursiveObject = new WithIdlRecursiveAnnotation();
        recursiveObject.data = "parent";
        recursiveObject.child = new WithIdlRecursiveAnnotation();
        recursiveObject.child.data = "child";
        recursiveObject.child.child = new WithIdlRecursiveAnnotation();
        recursiveObject.child.child.data = "grandchild";
        testRoundTripSerialize(recursiveObject);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testRecursiveStructWithoutRecursiveAnnotation()
            throws Exception
    {
        WithoutRecursiveAnnotation recursiveObject = new WithoutRecursiveAnnotation();
        recursiveObject.data = "parent";
        recursiveObject.child = new WithoutRecursiveAnnotation();
        recursiveObject.child.data = "child";
        recursiveObject.child.child = new WithoutRecursiveAnnotation();
        recursiveObject.child.child.data = "grandchild";
        testRoundTripSerialize(recursiveObject);
    }

    @Test
    public void testStructWithRecursionViaListElementTypes()
            throws Exception
    {
        ViaListElementType recursiveObject = new ViaListElementType();
        recursiveObject.data = "parent";
        recursiveObject.children = Lists.newArrayList(new ViaListElementType());
        recursiveObject.children.get(0).data = "child";
        recursiveObject.children.get(0).children = Lists.newArrayList(new ViaListElementType());
        recursiveObject.children.get(0).children.get(0).data = "grandchild";
        testRoundTripSerialize(recursiveObject);
    }

    @Test
    public void testStructWithRecursionViaNestedListElementTypes()
            throws Exception
    {
        ViaNestedListElementType recursiveObject = new ViaNestedListElementType();
        recursiveObject.data = "parent";
        recursiveObject.children = ImmutableList.of(new ArrayList<>());

        ViaNestedListElementType child = new ViaNestedListElementType();
        child.data = "child";
        recursiveObject.children.get(0).add(child);
        child.children = ImmutableList.of(new ArrayList<>());

        ViaNestedListElementType grandChild = new ViaNestedListElementType();
        grandChild.data = "grandchild";
        child.children.get(0).add(grandChild);

        testRoundTripSerialize(recursiveObject);
    }

    @Test
    void testStructWithRecursionViaMapKeyAndValueTypes()
            throws Exception
    {
        ViaMapKeyAndValueTypes recursiveObject = new ViaMapKeyAndValueTypes();
        recursiveObject.data = "parent";
        ViaMapKeyAndValueTypes keyChild = new ViaMapKeyAndValueTypes();
        keyChild.data = "keychild";
        ViaMapKeyAndValueTypes valueChild = new ViaMapKeyAndValueTypes();
        valueChild.data = "valuechild";
        recursiveObject.children = ImmutableMap.of(keyChild, valueChild);
        testRoundTripSerialize(recursiveObject);
    }

    @Test
    void testCoRecursive()
            throws Exception
    {
        CoRecursive recursiveObject = new CoRecursive();
        recursiveObject.data = "parent";
        recursiveObject.child = new CoRecursiveHelper();
        recursiveObject.child.data = "child";
        recursiveObject.child.child = new CoRecursive();
        recursiveObject.child.child.data = "grandchild";
        testRoundTripSerialize(recursiveObject);
    }

    @Test
    void testCoRecursiveStartingAtHelper()
            throws Exception
    {
        CoRecursiveHelper recursiveObject = new CoRecursiveHelper();
        recursiveObject.data = "parent";
        recursiveObject.child = new CoRecursive();
        recursiveObject.child.data = "child";
        recursiveObject.child.child = new CoRecursiveHelper();
        recursiveObject.child.child.data = "grandchild";
        testRoundTripSerialize(recursiveObject);
    }

    @Test
    void testCoRecursiveTree()
            throws Exception
    {
        {
            CoRecursiveTree recursiveLeaf = new CoRecursiveTree();
            recursiveLeaf.data = "grandchild";
            CoRecursiveTreeHelper recursiveNode = new CoRecursiveTreeHelper();
            recursiveNode.data = "child";
            recursiveNode.child = recursiveLeaf;
            CoRecursiveTree recursiveRoot = new CoRecursiveTree();
            recursiveRoot.data = "root";
            recursiveRoot.children = Lists.newArrayList(recursiveNode);
            testRoundTripSerialize(recursiveRoot);
        }

        {
            CoRecursiveTreeHelper recursiveLeaf = new CoRecursiveTreeHelper();
            recursiveLeaf.data = "grandchild";
            CoRecursiveTree recursiveNode = new CoRecursiveTree();
            recursiveNode.data = "child";
            recursiveNode.children = Lists.newArrayList(recursiveLeaf);
            CoRecursiveTreeHelper recursiveRoot = new CoRecursiveTreeHelper();
            recursiveRoot.data = "root";
            recursiveRoot.child = recursiveNode;
            testRoundTripSerialize(recursiveRoot);
        }
    }

    @Test
    public void testRecursiveUnion()
            throws Exception
    {
        RecursiveUnion recursiveUnion = new RecursiveUnion(new RecursiveUnion("child"));
        testRoundTripSerialize(recursiveUnion);
    }

    @Test
    public void testRecursiveDefaultUnion()
            throws Exception
    {
        RecursiveDefaultUnion recursiveDefaultUnion = new RecursiveDefaultUnion(new RecursiveDefaultUnion("child"));
        testRoundTripSerialize(recursiveDefaultUnion);
    }

    private void assertAllFieldsSet(IsSetBean isSetBean, boolean expected)
    {
        assertEquals(isSetBean.isBooleanSet(), expected);
        assertEquals(isSetBean.isByteSet(), expected);
        assertEquals(isSetBean.isShortSet(), expected);
        assertEquals(isSetBean.isIntegerSet(), expected);
        assertEquals(isSetBean.isLongSet(), expected);
        assertEquals(isSetBean.isDoubleSet(), expected);
        assertEquals(isSetBean.isStringSet(), expected);
        assertEquals(isSetBean.isStructSet(), expected);
        assertEquals(isSetBean.isSetSet(), expected);
        assertEquals(isSetBean.isListSet(), expected);
        assertEquals(isSetBean.isMapSet(), expected);
        assertEquals(!ByteBuffer.wrap("empty".getBytes(UTF_8)).equals(isSetBean.field), expected);
    }

    private <T> void testRoundTripSerialize(T value)
            throws Exception
    {
        testRoundTripSerialize(value, x -> {});
    }

    private <T> void testRoundTripSerialize(T value, Consumer<T> consumer)
            throws Exception
    {
        consumer.accept(testRoundTripSerialize(value, TBinaryProtocol::new));
        consumer.accept(testRoundTripSerialize(value, TCompactProtocol::new));
        consumer.accept(testRoundTripSerialize(value, TFacebookCompactProtocol::new));
    }

    private <T> T testRoundTripSerialize(T value, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        ThriftCodec<T> readCodec = (ThriftCodec<T>) readCodecManager.getCodec(value.getClass());
        ThriftCodec<T> writeCodec = (ThriftCodec<T>) writeCodecManager.getCodec(value.getClass());

        return testRoundTripSerialize(readCodec, writeCodec, value, protocolFactory);
    }

    private <T> void testRoundTripSerialize(TypeToken<T> typeToken, T value)
            throws Exception
    {
        testRoundTripSerialize(typeToken, value, TBinaryProtocol::new);
        testRoundTripSerialize(typeToken, value, TCompactProtocol::new);
        testRoundTripSerialize(typeToken, value, TFacebookCompactProtocol::new);
    }

    private <T> void testRoundTripSerialize(TypeToken<T> typeToken, T value, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        ThriftCodec<T> readCodec = (ThriftCodec<T>) readCodecManager.getCodec(typeToken.getType());
        ThriftCodec<T> writeCodec = (ThriftCodec<T>) writeCodecManager.getCodec(typeToken.getType());

        testRoundTripSerialize(readCodec, writeCodec, typeToken.getType(), value, protocolFactory);
    }

    private <T> void testRoundTripSerialize(ThriftCodec<T> readCodec, ThriftCodec<T> writeCodec, T structInstance)
            throws Exception
    {
        testRoundTripSerialize(readCodec, writeCodec, structInstance, TBinaryProtocol::new);
        testRoundTripSerialize(readCodec, writeCodec, structInstance, TCompactProtocol::new);
        testRoundTripSerialize(readCodec, writeCodec, structInstance, TFacebookCompactProtocol::new);
    }

    private <T> T testRoundTripSerialize(ThriftCodec<T> readCodec, ThriftCodec<T> writeCodec, T structInstance, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        Class<T> structClass = (Class<T>) structInstance.getClass();
        return testRoundTripSerialize(readCodec, writeCodec, structClass, structInstance, protocolFactory);
    }

    private <T> T testRoundTripSerialize(ThriftCodec<T> readCodec, ThriftCodec<T> writeCodec, Type structType, T structInstance, Function<TTransport, TProtocol> protocolFactory)
            throws Exception
    {
        ThriftCatalog readCatalog = readCodecManager.getCatalog();
        ThriftStructMetadata readMetadata = readCatalog.getThriftStructMetadata(structType);
        assertNotNull(readMetadata);

        ThriftCatalog writeCatalog = writeCodecManager.getCatalog();
        ThriftStructMetadata writeMetadata = writeCatalog.getThriftStructMetadata(structType);
        assertNotNull(writeMetadata);

        TMemoryBuffer transport = new TMemoryBuffer(10 * 1024);
        TProtocol protocol = protocolFactory.apply(transport);
        writeCodec.write(structInstance, protocol);

        T copy = readCodec.read(protocol);
        assertNotNull(copy);
        assertEquals(copy, structInstance);

        return copy;
    }

    private OneOfEverything createOneOfEverything()
    {
        OneOfEverything one = new OneOfEverything();
        one.aBoolean = true;
        one.aByte = 11;
        one.aShort = 22;
        one.aInt = 33;
        one.aLong = 44;
        one.aDouble = 55;
        one.aString = "message";
        one.aStruct = new BonkField("struct", 66);
        one.aEnum = Fruit.CHERRY;
        one.aCustomEnum = Letter.C;

        one.aBooleanSet = ImmutableSet.of(true, false);
        one.aByteSet = ImmutableSet.of((byte) -1, (byte) 0, (byte) 1);
        one.aShortSet = ImmutableSet.of((short) -1, (short) 0, (short) 1);
        one.aIntegerSet = ImmutableSet.of(-1, 0, 1);
        one.aLongSet = ImmutableSet.of(-1L, 0L, 1L);
        one.aDoubleSet = ImmutableSet.of(-42.1d, 0.0d, 42.1d);
        one.aStringSet = ImmutableSet.of("a", "string", "set");
        one.aStructSet = ImmutableSet.of(new BonkField("message", 42), new BonkField("other", 11));
        one.aEnumSet = ImmutableSet.copyOf(Fruit.values());
        one.aCustomEnumSet = ImmutableSet.copyOf(Letter.values());

        one.aBooleanList = ImmutableList.of(true, false);
        one.aByteList = ImmutableList.of((byte) -1, (byte) 0, (byte) 1);
        one.aShortList = ImmutableList.of((short) -1, (short) 0, (short) 1);
        one.aIntegerList = ImmutableList.of(-1, 0, 1);
        one.aLongList = ImmutableList.of(-1L, 0L, 1L);
        one.aDoubleList = ImmutableList.of(-42.1d, 0.0d, 42.1d);
        one.aStringList = ImmutableList.of("a", "string", "list");
        one.aStructList = ImmutableList.of(new BonkField("message", 42), new BonkField("other", 11));
        one.aEnumList = ImmutableList.copyOf(Fruit.values());
        one.aCustomEnumList = ImmutableList.copyOf(Letter.values());

        one.aBooleanValueMap = ImmutableMap.of("TRUE", true, "FALSE", false);
        one.aByteValueMap = ImmutableMap.of("-1", (byte) -1, "0", (byte) 0, "1", (byte) 1);
        one.aShortValueMap = ImmutableMap.of("-1", (short) -1, "0", (short) 0, "1", (short) 1);
        one.aIntegerValueMap = ImmutableMap.of("-1", -1, "0", 0, "1", 1);
        one.aLongValueMap = ImmutableMap.of("-1", -1L, "0", 0L, "1", 1L);
        one.aDoubleValueMap = ImmutableMap.of("neg", -42.1d, "0", 0.0d, "pos", 42.1d);
        one.aStringValueMap = ImmutableMap.of("1", "a", "2", "string", "3", "map");
        one.aStructValueMap = ImmutableMap.of("main", new BonkField("message", 42), "other", new BonkField("other", 11));
        one.aEnumValueMap = ImmutableMap.of("apple", Fruit.APPLE, "banana", Fruit.BANANA);
        one.aCustomEnumValueMap = ImmutableMap.of("a", Letter.A, "b", Letter.B);

        one.aBooleanKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aBooleanValueMap).inverse());
        one.aByteKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aByteValueMap).inverse());
        one.aShortKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aShortValueMap).inverse());
        one.aIntegerKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aIntegerValueMap).inverse());
        one.aLongKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aLongValueMap).inverse());
        one.aDoubleKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aDoubleValueMap).inverse());
        one.aStringKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aStringValueMap).inverse());
        one.aStructKeyMap = ImmutableMap.copyOf(HashBiMap.create(one.aStructValueMap).inverse());
        one.aEnumKeyMap = ImmutableMap.of(Fruit.APPLE, "apple", Fruit.BANANA, "banana");
        one.aCustomEnumKeyMap = ImmutableMap.of(Letter.A, "a", Letter.B, "b");

        one.aBooleanOptional = Optional.of(true);
        one.aByteOptional = Optional.of((byte) -1);
        one.aShortOptional = Optional.of((short) -1);
        one.aIntegerOptional = Optional.of(-1);
        one.aLongOptional = Optional.of(-1L);
        one.aDoubleOptional = Optional.of(-42.1d);
        one.aStringOptional = Optional.of("a");
        one.aStructOptional = Optional.of(new BonkField("message", 42));
        one.aEnumOptional = Optional.of(Fruit.BANANA);
        one.aCustomEnumOptional = Optional.of(Letter.C);

        one.aSetOfListsOfMaps = ImmutableSet.of(
                ImmutableList.of(
                        ImmutableMap.of(
                                "1: main", new BonkField("message", 42),
                                "1: other", new BonkField("other", 11)),
                        ImmutableMap.of(
                                "1: main", new BonkField("message", 42),
                                "1: other", new BonkField("other", 11))),
                ImmutableList.of(
                        ImmutableMap.of(
                                "2: main", new BonkField("message", 42),
                                "2: other", new BonkField("other", 11)),
                        ImmutableMap.of(
                                "2: main", new BonkField("message", 42),
                                "2: other", new BonkField("other", 11))));

        one.aMapOfListToSet = ImmutableMap.of(
                ImmutableList.of("a", "b"),
                ImmutableSet.of(
                        new BonkField("1: message", 42),
                        new BonkField("1: other", 11)),
                ImmutableList.of("c", "d"),
                ImmutableSet.of(
                        new BonkField("2: message", 42),
                        new BonkField("2: other", 11)));

        one.aUnion = new UnionField("Hello, World");

        one.aUnionSet = ImmutableSet.of(new UnionField("Hello, World"), new UnionField(123456L), new UnionField(Fruit.CHERRY));
        one.aUnionList = ImmutableList.of(new UnionField("Hello, World"), new UnionField(123456L), new UnionField(Fruit.CHERRY));

        one.aUnionKeyMap = ImmutableMap.of(new UnionField("Hello, World"), "Eins",
                new UnionField(123456L), "Zwei",
                new UnionField(Fruit.CHERRY), "Drei");

        one.aUnionValueMap = ImmutableMap.of("Eins", new UnionField("Hello, World"),
                "Zwei", new UnionField(123456L),
                "Drei", new UnionField(Fruit.CHERRY));

        return one;
    }
}
