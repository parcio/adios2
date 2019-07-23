/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 16, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAMETADATA_H_
#define ADIOS2_ENGINE_JULEAMETADATA_H_

#include <julea.h>
#include <complex.h>

namespace adios2
{
namespace core
{
namespace engine
{

#ifdef __cplusplus
typedef std::complex<float> cfloat;
typedef std::complex<double> cdouble;
#else
typedef float _Complex cfloat;
typedef double _Complex cdouble;
#endif

/* ADIOS Types in ADIOS2-2.4.0 */
enum variable_type
{
    STRING,
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT,
    DOUBLE,
    LONG_DOUBLE,
    COMPLEX_FLOAT,
    COMPLEX_DOUBLE
};
typedef enum variable_type variable_type;

union value_type
{
    // char *string; // TODO: needed? what would be the "minimum" of a string
    int8_t integer_8;
    uint8_t u_integer_8;
    int16_t integer_16;
    uint16_t u_integer_16;
    int32_t integer_32;
    uint32_t u_integer_32;
    int64_t integer_64;
    uint64_t u_integer_64;
    float real_float;
    double real_double;
    long double real_long_double;
    cfloat complex_float;
    cdouble complex_double;
};
typedef union value_type value_type;

/**
 * Metadata information to be stored in kv store or structured metadata backend.
 *
 * TODO: VariableBase.h members are stored in JULEA but are currently not used
 * in ADIOS.
 *
 */
struct Metadata
{
    char *name;

    unsigned long *shape;
    unsigned long *start;
    unsigned long *count;
    unsigned long *memory_start; // TODO -> only for inline engine?
    unsigned long *memory_count; // TODO -> only for inline engine?

    unsigned long shape_size;
    unsigned long start_size;
    unsigned long count_size;
    unsigned long memory_start_size; // TODO -> only for inline engine?
    unsigned long memory_count_size; // TODO -> only for inline engine?

    size_t steps_start;
    size_t steps_count;
    size_t block_id;              // TODO
    size_t index_start;           // VariableBase.h TODO
    size_t element_size;          // VariableBase.h TODO
    size_t available_steps_start; // VariableBase.h TODO
    size_t available_steps_count; // VariableBase.h TODO

    variable_type var_type;

    value_type min_value;           // not for strings
    value_type max_value;           // not for strings
    value_type curr_value;          // not for strings

    value_type *min_value_ptr;

    unsigned int sizeof_var_type; // store the actual size of the variable type
    unsigned int data_size;
    // unsigned int deferred_counter; //VariableBase.h TODO: implement!

    bool is_value;
    bool is_single_value;         // VariableBase.h TODO
    bool is_constant_dims;        // VariableBase.h TODO? //protected
    bool is_read_as_joined;       // VariableBase.h TODO
    bool is_read_as_local_value;  // VariableBase.h TODO
    bool is_random_access;        // VariableBase.h TODO
    bool is_first_streaming_step; // VariableBase.h TODO

    // TODO: ShapeID m_ShapeID; see shape types in ADIOSTypes.h
    // TODO: Operations Map
    // size_t m_IndexStart = 0; TODO: needed?
};
typedef struct Metadata Metadata;

struct AttributeMetadata
{
    char *name;
    variable_type attr_type;
    size_t number_elements;
    bool is_single_value;
    guint data_size;
};
typedef struct AttributeMetadata AttributeMetadata;

struct JuleaInfo
{
    JSemantics *semantics;
    char *name_space;
};
typedef struct JuleaInfo JuleaInfo;

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAMETADATA_H_ */
