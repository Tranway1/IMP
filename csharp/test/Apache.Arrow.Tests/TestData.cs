// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Apache.Arrow.Types;
using System;
using System.Collections.Generic;

namespace Apache.Arrow.Tests
{
    public static class TestData
    {
        public static RecordBatch CreateSampleRecordBatch(int length)
        {
            return CreateSampleRecordBatch(length, columnSetCount: 1);
        }

        public static RecordBatch CreateSampleRecordBatch(int length, int columnSetCount)
        {
            Schema.Builder builder = new Schema.Builder();
            for (int i = 0; i < columnSetCount; i++)
            {
                builder.Field(CreateField(BooleanType.Default, i));
                builder.Field(CreateField(UInt8Type.Default, i));
                builder.Field(CreateField(Int8Type.Default, i));
                builder.Field(CreateField(UInt16Type.Default, i));
                builder.Field(CreateField(Int16Type.Default, i));
                builder.Field(CreateField(UInt32Type.Default, i));
                builder.Field(CreateField(Int32Type.Default, i));
                builder.Field(CreateField(UInt64Type.Default, i));
                builder.Field(CreateField(Int64Type.Default, i));
                builder.Field(CreateField(FloatType.Default, i));
                builder.Field(CreateField(DoubleType.Default, i));
                builder.Field(CreateField(Date32Type.Default, i));
                builder.Field(CreateField(Date64Type.Default, i));
                builder.Field(CreateField(TimestampType.Default, i));
                builder.Field(CreateField(StringType.Default, i));
                //builder.Field(CreateField(new FixedSizeBinaryType(16), i));
                //builder.Field(CreateField(new DecimalType(19, 2)));
                //builder.Field(CreateField(HalfFloatType.Default));
                //builder.Field(CreateField(StringType.Default));
                //builder.Field(CreateField(Time32Type.Default));
                //builder.Field(CreateField(Time64Type.Default));

                builder.Field(CreateField(new ListType(Int64Type.Default), i));
            }

            Schema schema = builder.Build();

            IEnumerable<IArrowArray> arrays = CreateArrays(schema, length);

            return new RecordBatch(schema, arrays, length);
        }

        private static Field CreateField(ArrowType type, int iteration)
        {
            return new Field(type.Name + iteration, type, nullable: false);
        }

        private static IEnumerable<IArrowArray> CreateArrays(Schema schema, int length)
        {
            int fieldCount = schema.Fields.Count;
            List<IArrowArray> arrays = new List<IArrowArray>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                Field field = schema.GetFieldByIndex(i);
                arrays.Add(CreateArray(field, length));
            }
            return arrays;
        }

        private static IArrowArray CreateArray(Field field, int length)
        {
            var creator = new ArrayCreator(length);

            field.DataType.Accept(creator);

            return creator.Array;
        }

        private class ArrayCreator :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<Date32Type>,
            IArrowTypeVisitor<Date64Type>,
            IArrowTypeVisitor<Int8Type>,
            IArrowTypeVisitor<Int16Type>,
            IArrowTypeVisitor<Int32Type>,
            IArrowTypeVisitor<Int64Type>,
            IArrowTypeVisitor<UInt8Type>,
            IArrowTypeVisitor<UInt16Type>,
            IArrowTypeVisitor<UInt32Type>,
            IArrowTypeVisitor<UInt64Type>,
            IArrowTypeVisitor<FloatType>,
            IArrowTypeVisitor<DoubleType>,
            IArrowTypeVisitor<TimestampType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<ListType>
        {
            private int Length { get; }
            public IArrowArray Array { get; private set; }

            public ArrayCreator(int length)
            {
                Length = length;
            }

            public void Visit(BooleanType type) => GenerateArray(new BooleanArray.Builder(), x => x % 2 == 0);
            public void Visit(Int8Type type) => GenerateArray(new Int8Array.Builder(), x => (sbyte)x);
            public void Visit(Int16Type type) => GenerateArray(new Int16Array.Builder(), x => (short)x);
            public void Visit(Int32Type type) => GenerateArray(new Int32Array.Builder(), x => x);
            public void Visit(Int64Type type) => GenerateArray(new Int64Array.Builder(), x => x);
            public void Visit(UInt8Type type) => GenerateArray(new UInt8Array.Builder(), x => (byte)x);
            public void Visit(UInt16Type type) => GenerateArray(new UInt16Array.Builder(), x => (ushort)x);
            public void Visit(UInt32Type type) => GenerateArray(new UInt32Array.Builder(), x => (uint)x);
            public void Visit(UInt64Type type) => GenerateArray(new UInt64Array.Builder(), x => (ulong)x);
            public void Visit(FloatType type) => GenerateArray(new FloatArray.Builder(), x => ((float)x / Length));
            public void Visit(DoubleType type) => GenerateArray(new DoubleArray.Builder(), x => ((double)x / Length));

            public void Visit(Date32Type type)
            {
                var builder = new Date32Array.Builder().Reserve(Length);

                // Length can be greater than the number of days since DateTime.MinValue.
                // Set a cap for how many days can be subtracted from now.
                int maxDays = Math.Min(Length, 100_000);
                var basis = DateTimeOffset.UtcNow.AddDays(-maxDays);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(basis.AddDays(i % maxDays));
                }

                Array = builder.Build();
            }

            public void Visit(Date64Type type)
            {
                var builder = new Date64Array.Builder().Reserve(Length);
                var basis = DateTimeOffset.UtcNow.AddSeconds(-Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(basis.AddSeconds(i));
                }

                Array = builder.Build();
            }

            public void Visit(TimestampType type)
            {
                var builder = new TimestampArray.Builder().Reserve(Length);
                var basis = DateTimeOffset.UtcNow.AddMilliseconds(-Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(basis.AddMilliseconds(i));
                }

                Array = builder.Build();
            }

            public void Visit(StringType type)
            {
                var str = "hello";
                var builder = new StringArray.Builder();

                for (var i = 0; i < Length; i++)
                {
                    builder.Append(str);
                }

                Array = builder.Build();
            }

            public void Visit(ListType type)
            {
                var builder = new ListArray.Builder(type.ValueField).Reserve(Length);

                //Todo : Support various types
                var valueBuilder = (Int64Array.Builder)builder.ValueBuilder.Reserve(Length);

                for (var i = 0; i < Length; i++)
                {
                    builder.Append();
                    valueBuilder.Append(i);
                }

                Array = builder.Build();

            }

            private void GenerateArray<T, TArray, TArrayBuilder>(IArrowArrayBuilder<T, TArray, TArrayBuilder> builder, Func<int, T> generator)
                where TArrayBuilder : IArrowArrayBuilder<T, TArray, TArrayBuilder>
                where TArray : IArrowArray
                where T : struct
            {
                for (var i = 0; i < Length; i++)
                {
                    var value = generator(i);
                    builder.Append(value);
                }

                Array = builder.Build(default);
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException();
            }
        }
    }
}
