
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/types.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <math.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>

#include <iostream>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "gtest/gtest_pred_impl.h"
#include "olap/hll.h"
#include "runtime/descriptors.h"
#include "runtime/types.cpp"
#include "testutil/desc_tbl_builder.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/bitmap_value.h"
#include "util/quantile_state.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_bitmap.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_or_datetime_v2.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_ipv4.h"
#include "vec/data_types/data_type_ipv6.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_quantilestate.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/sink/vmysql_result_writer.cpp"
#include "vec/sink/vmysql_result_writer.h"

namespace doris::vectorized {

class TestBlockSerializer final : public vectorized::MySQLResultBlockBuffer {
public:
    TestBlockSerializer(RuntimeState* state) : vectorized::MySQLResultBlockBuffer(state) {}
    ~TestBlockSerializer() override = default;
    std::shared_ptr<TFetchDataResult> get_block() {
        std::lock_guard<std::mutex> l(_lock);
        DCHECK_EQ(_result_batch_queue.size(), 1);
        auto result = std::move(_result_batch_queue.front());
        _result_batch_queue.pop_front();
        return result;
    }
};

void serialize_and_deserialize_mysql_test() {
    vectorized::Block block;
    //    create_descriptor_tablet();
    std::vector<std::tuple<std::string, FieldType, int, PrimitiveType, bool>> cols {
            {"k1", FieldType::OLAP_FIELD_TYPE_INT, 1, TYPE_INT, false},
            {"k7", FieldType::OLAP_FIELD_TYPE_INT, 7, TYPE_INT, true},
            {"k2", FieldType::OLAP_FIELD_TYPE_STRING, 2, TYPE_STRING, false},
            {"k3", FieldType::OLAP_FIELD_TYPE_DECIMAL128I, 3, TYPE_DECIMAL128I, false},
            {"k11", FieldType::OLAP_FIELD_TYPE_DATETIME, 11, TYPE_DATETIME, false},
            {"k4", FieldType::OLAP_FIELD_TYPE_BOOL, 4, TYPE_BOOLEAN, false},
            {"k5", FieldType::OLAP_FIELD_TYPE_IPV4, 5, TYPE_IPV4, false},
            {"k6", FieldType::OLAP_FIELD_TYPE_IPV6, 6, TYPE_IPV6, false}};
    int row_num = 7;
    // make desc and generate block
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
    _output_vexpr_ctxs.resize(cols.size());
    doris::RuntimeState runtime_stat;
    ObjectPool object_pool;
    int col_idx = 0;
    for (auto t : cols) {
        TSlotDescriptor tslot;
        RowDescriptor rowDescriptor;
        std::vector<TExprNode> nodes;
        nodes.resize(1);
        std::string col_name = std::get<0>(t);
        tslot.__set_colName(col_name);
        auto type_desc = DataTypeFactory::instance().create_data_type(
                std::get<3>(t), false, TYPE_DECIMAL128I == std::get<3>(t) ? 27 : 0,
                TYPE_DECIMAL128I == std::get<3>(t) ? 9 : 0);
        bool is_nullable(std::get<4>(t));
        switch (std::get<3>(t)) {
        case TYPE_BOOLEAN:
            tslot.__set_slotType(type_desc->to_thrift());
            {
                auto vec = vectorized::ColumnVector<TYPE_BOOLEAN>::create();
                auto& data = vec->get_data();
                for (int i = 0; i < row_num; ++i) {
                    data.push_back(i % 2);
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeUInt8>());
                vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type,
                                                                col_name);
                block.insert(std::move(type_and_name));
            }
            break;
        case TYPE_INT:
            tslot.__set_slotType(type_desc->to_thrift());
            if (is_nullable) {
                {
                    auto column_vector_int32 = vectorized::ColumnVector<TYPE_INT>::create();
                    auto column_nullable_vector =
                            vectorized::make_nullable(std::move(column_vector_int32));
                    auto mutable_nullable_vector = std::move(*column_nullable_vector).mutate();
                    for (int i = 0; i < row_num; i++) {
                        mutable_nullable_vector->insert(Field::create_field<TYPE_INT>(int32_t(i)));
                    }
                    auto data_type = vectorized::make_nullable(
                            std::make_shared<vectorized::DataTypeInt32>());
                    vectorized::ColumnWithTypeAndName type_and_name(
                            mutable_nullable_vector->get_ptr(), data_type, col_name);
                    block.insert(type_and_name);
                }
            } else {
                auto vec = vectorized::ColumnVector<TYPE_INT>::create();
                auto& data = vec->get_data();
                for (int i = 0; i < row_num; ++i) {
                    data.push_back(i);
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeInt32>());
                vectorized::ColumnWithTypeAndName type_and_name(vec->get_ptr(), data_type,
                                                                col_name);
                block.insert(std::move(type_and_name));
            }
            break;
        case TYPE_DECIMAL128I: {
            vectorized::DataTypePtr decimal_data_type(
                    doris::vectorized::create_decimal(27, 9, true));
            type_desc = decimal_data_type;
            tslot.__set_slotType(type_desc->to_thrift());
            auto decimal_column = decimal_data_type->create_column();
            auto& data = ((vectorized::ColumnDecimal128V3*)decimal_column.get())->get_data();
            for (int i = 0; i < row_num; ++i) {
                auto value = __int128_t(i * pow(10, 9) + i * pow(10, 8));
                data.push_back(value);
            }
            vectorized::ColumnWithTypeAndName type_and_name(decimal_column->get_ptr(),
                                                            decimal_data_type, col_name);
            block.insert(type_and_name);
        } break;
        case TYPE_STRING:
            tslot.__set_slotType(type_desc->to_thrift());
            {
                auto strcol = vectorized::ColumnString::create();
                for (int i = 0; i < row_num; ++i) {
                    std::string is = std::to_string(i);
                    strcol->insert_data(is.c_str(), is.size());
                }
                vectorized::DataTypePtr data_type(std::make_shared<vectorized::DataTypeString>());
                vectorized::ColumnWithTypeAndName type_and_name(strcol->get_ptr(), data_type,
                                                                col_name);
                block.insert(type_and_name);
            }
            break;
        case TYPE_HLL:
            tslot.__set_slotType(type_desc->to_thrift());
            {
                vectorized::DataTypePtr hll_data_type(std::make_shared<vectorized::DataTypeHLL>());
                auto hll_column = hll_data_type->create_column();
                std::vector<HyperLogLog>& container =
                        ((vectorized::ColumnHLL*)hll_column.get())->get_data();
                for (int i = 0; i < row_num; ++i) {
                    HyperLogLog hll;
                    hll.update(i);
                    container.push_back(hll);
                }
                vectorized::ColumnWithTypeAndName type_and_name(hll_column->get_ptr(),
                                                                hll_data_type, col_name);

                block.insert(type_and_name);
            }
            break;
        case TYPE_DATEV2:
            tslot.__set_slotType(type_desc->to_thrift());
            {
                auto column_vector_date_v2 = vectorized::ColumnVector<TYPE_DATEV2>::create();
                auto& date_v2_data = column_vector_date_v2->get_data();
                for (int i = 0; i < row_num; ++i) {
                    DateV2Value<DateV2ValueType> value;
                    value.unchecked_set_time(2022, 6, 6, 0, 0, 0, 0);
                    date_v2_data.push_back(*reinterpret_cast<vectorized::UInt32*>(&value));
                }
                vectorized::DataTypePtr date_v2_type(
                        std::make_shared<vectorized::DataTypeDateV2>());
                vectorized::ColumnWithTypeAndName test_date_v2(column_vector_date_v2->get_ptr(),
                                                               date_v2_type, col_name);
                block.insert(test_date_v2);
            }
            break;
        case TYPE_DATE: // int64
            tslot.__set_slotType(type_desc->to_thrift());
            {
                auto column_vector_date = vectorized::ColumnVector<TYPE_DATE>::create();
                auto& date_data = column_vector_date->get_data();
                for (int i = 0; i < row_num; ++i) {
                    VecDateTimeValue value;
                    value.from_date_int64(20210501);
                    date_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
                }
                vectorized::DataTypePtr date_type(std::make_shared<vectorized::DataTypeDate>());
                vectorized::ColumnWithTypeAndName test_date(column_vector_date->get_ptr(),
                                                            date_type, col_name);
                block.insert(test_date);
            }
            break;
        case TYPE_DATETIME: // int64
            tslot.__set_slotType(type_desc->to_thrift());
            {
                auto column_vector_datetime = vectorized::ColumnVector<TYPE_DATETIME>::create();
                auto& datetime_data = column_vector_datetime->get_data();
                for (int i = 0; i < row_num; ++i) {
                    VecDateTimeValue value;
                    value.from_date_int64(20210501080910);
                    datetime_data.push_back(*reinterpret_cast<vectorized::Int64*>(&value));
                }
                vectorized::DataTypePtr datetime_type(
                        std::make_shared<vectorized::DataTypeDateTime>());
                vectorized::ColumnWithTypeAndName test_datetime(column_vector_datetime->get_ptr(),
                                                                datetime_type, col_name);
                block.insert(test_datetime);
            }
            break;
        case TYPE_IPV4:
            tslot.__set_slotType(type_desc->to_thrift());
            {
                auto column_vector_ipv4 = vectorized::ColumnVector<TYPE_IPV4>::create();
                auto& ipv4_data = column_vector_ipv4->get_data();
                for (int i = 0; i < row_num; ++i) {
                    IPv4Value ipv4_value;
                    bool res = ipv4_value.from_string("192.168.0." + std::to_string(i));
                    ASSERT_TRUE(res);
                    ipv4_data.push_back(ipv4_value.value());
                }
                vectorized::DataTypePtr ipv4_type(std::make_shared<vectorized::DataTypeIPv4>());
                vectorized::ColumnWithTypeAndName test_ipv4(column_vector_ipv4->get_ptr(),
                                                            ipv4_type, col_name);
                block.insert(test_ipv4);
            }
            break;
        case TYPE_IPV6:
            tslot.__set_slotType(type_desc->to_thrift());
            {
                auto column_vector_ipv6 = vectorized::ColumnVector<TYPE_IPV6>::create();
                auto& ipv6_data = column_vector_ipv6->get_data();
                for (int i = 0; i < row_num; ++i) {
                    IPv6Value ipv6_value;
                    bool res = ipv6_value.from_string("2001:2000:3080:1351::" + std::to_string(i));
                    ASSERT_TRUE(res);
                    ipv6_data.push_back(ipv6_value.value());
                }
                vectorized::DataTypePtr ipv6_type(std::make_shared<vectorized::DataTypeIPv6>());
                vectorized::ColumnWithTypeAndName test_ipv6(column_vector_ipv6->get_ptr(),
                                                            ipv6_type, col_name);
                block.insert(test_ipv6);
            }
            break;
        default:
            break;
        }

        tslot.__set_col_unique_id(std::get<2>(t));
        TSlotRef slotRef;
        slotRef.__set_slot_id(tslot.id);
        slotRef.__set_col_unique_id(tslot.col_unique_id);
        slotRef.__set_tuple_id(tslot.slotIdx);
        nodes[0].__set_slot_ref(slotRef);
        nodes[0].__set_node_type(TExprNodeType::SLOT_REF);
        nodes[0].__set_type(create_type_desc(std::get<3>(t), type_desc->get_precision(),
                                             type_desc->get_scale()));
        TExpr texpr;
        texpr.__set_nodes(nodes);
        VExprContextSPtr ctx = nullptr;
        Status st = VExpr::create_expr_tree(texpr, ctx);
        std::cout << st.to_string() << std::endl;
        doris::DescriptorTblBuilder builder(&object_pool);
        builder.declare_tuple() << type_desc;
        doris::DescriptorTbl* desc_tbl = builder.build();
        auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
        doris::RowDescriptor row_desc(tuple_desc, false);
        runtime_stat.set_desc_tbl(desc_tbl);
        st = ctx->prepare(&runtime_stat, row_desc);
        std::cout << st.to_string() << std::endl;
        EXPECT_TRUE(st.ok());
        _output_vexpr_ctxs[col_idx] = ctx;
        ++col_idx;
    }

    // serialize
    std::cout << "block structure: " << block.dump_structure() << std::endl;

    // mysql_writer init
    MockRuntimeState state;
    auto serializer = std::make_shared<TestBlockSerializer>(&state);
    vectorized::VMysqlResultWriter<false> mysql_writer(serializer, _output_vexpr_ctxs, nullptr);

    Status st = mysql_writer.write(&runtime_stat, block);
    EXPECT_TRUE(st.ok());
}

TEST(DataTypeSerDeMysqlTest, ScalaSerDeTest) {
    serialize_and_deserialize_mysql_test();
}

} // namespace doris::vectorized
