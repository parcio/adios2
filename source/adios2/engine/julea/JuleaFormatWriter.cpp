/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 22, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEAFORMATWRITER_
#define ADIOS2_ENGINE_JULEAFORMATWRITER_

#include "JuleaWriter.h"
#include "JuleaFormatWriter.h"

#include <adios2_c.h>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

namespace adios2
{
namespace core
{
namespace engine
{



/**
 * Parsing the variable types to enum defined in JULEA's Adios Client.
 * Great that types are handled as string here...
 */
template <class T>
void parse_variable_type(Variable<T> &variable,
                         const typename Variable<T>::Info &blockInfo,
                         Metadata *metadata)
{

    if (helper::GetType<T>() == "string")
    {
        metadata->var_type = STRING;
        // metadata->min_value.string = blockInfo.Min;
    }
    else if (helper::GetType<T>() == "int8_t")
    {
        metadata->var_type = INT8;
        metadata->sizeof_var_type = sizeof(int8_t);
    }
    else if (helper::GetType<T>() == "uint8_t")
    {
        metadata->var_type = UINT8;
        metadata->sizeof_var_type = sizeof(uint8_t);
    }
    else if (helper::GetType<T>() == "int16_t")
    {
        metadata->var_type = INT16;
        metadata->sizeof_var_type = sizeof(int16_t);
        // metadata->min_value = (short) variable.m_Min;
        // metadata->min_value.shorter = static_cast<short>(variable.m_Min);
        // metadata->min_value.shorter = reinterpret_cast<T>(variable.m_Min);
    }
    else if (helper::GetType<T>() == "uint16_t")
    {
        metadata->var_type = UINT16;
        metadata->sizeof_var_type = sizeof(uint16_t);
    }
    else if (helper::GetType<T>() == "int32_t")
    {
        metadata->var_type = INT32;
        metadata->sizeof_var_type = sizeof(int32_t);
    }
    else if (helper::GetType<T>() == "uint32_t")
    {
        metadata->var_type = UINT32;
        metadata->sizeof_var_type = sizeof(uint32_t);
    }
    else if (helper::GetType<T>() == "int64_t")
    {
        metadata->var_type = INT64;
        metadata->sizeof_var_type = sizeof(int64_t);
    }
    else if (helper::GetType<T>() == "uint64_t")
    {
        metadata->var_type = UINT64;
        metadata->sizeof_var_type = sizeof(uint64_t);
    }
    else if (helper::GetType<T>() == "float")
    {
        metadata->var_type = FLOAT;
        metadata->sizeof_var_type = sizeof(float);
    }
    else if (helper::GetType<T>() == "double")
    {
        metadata->var_type = DOUBLE;
        metadata->sizeof_var_type = sizeof(double);
    }
    else if (helper::GetType<T>() == "long double")
    {
        metadata->var_type = LONG_DOUBLE;
        metadata->sizeof_var_type = sizeof(long double);
    }

    else if (helper::GetType<T>() == "float complex")
    {
        metadata->var_type = COMPLEX_FLOAT;
        // metadata->sizeof_var_type = sizeof(float complex); //TODO
    }
    else if (helper::GetType<T>() == "double complex")
    {
        metadata->var_type = COMPLEX_DOUBLE;
        // metadata->sizeof_var_type = sizeof(double complex); //TODO
    }
}



template <class T>
void parse_variable_type(Variable<T> &variable, const T *data,
                         Metadata *metadata)
{

    if (helper::GetType<T>() == "string")
    {
        metadata->var_type = STRING;
        // metadata->min_value.string = variable.Min();
    }
    else if (helper::GetType<T>() == "int8_t")
    {
        metadata->var_type = INT8;
        metadata->sizeof_var_type = sizeof(int8_t);
        // metadata->min_value.integer_8 = variable.Min();
    }
    else if (helper::GetType<T>() == "uint8_t")
    {
        metadata->var_type = UINT8;
        metadata->sizeof_var_type = sizeof(uint8_t);
        // metadata->min_value.u_integer_8 = variable.Min();
    }
    else if (helper::GetType<T>() == "int16_t")
    {
        metadata->var_type = INT16;
        metadata->sizeof_var_type = sizeof(int16_t);
        // adios2_variable_min((void*) metadata->min_value_ptr->integer_16,
        // variable); metadata->min_value_ptr->integer_16 = variable.Min();
        // metadata->min_value.integer_16 = 42;
        // metadata->min_value.integer_16 = (int) variable.Min();
        // metadata->min_value = (short) variable.m_Min;
        // metadata->min_value.integer_16 = static_cast<int>(variable.m_Min);
        // metadata->min_value.shorter = reinterpret_cast<T>(variable.m_Min);
        // metadata->min_value.integer_16 =
        // reinterpret_cast<int>(variable.m_Min); metadata->min_value.integer_16
        // = dynamic_cast<int>(variable.m_Min);
        // metadata->min_value_ptr->integer_16_ptr =
        // dynamic_cast<int>(variable.m_Min);

        // const adios2::core::VariableBase *variableBase =
        // reinterpret_cast<const adios2::core::VariableBase*>(&variable);
        // metadata->min_value.integer_16 = dynamic_cast<const
        // adios2::core::Variable<T> *>(variableBase)->m_Min;
    }
    else if (helper::GetType<T>() == "uint16_t")
    {
        metadata->var_type = UINT16;
        metadata->sizeof_var_type = sizeof(uint16_t);
    }
    else if (helper::GetType<T>() == "int32_t")
    {
        metadata->var_type = INT32;
        metadata->sizeof_var_type = sizeof(int32_t);
        // metadata->min_value.integer_32 = variable.Min();
    }
    else if (helper::GetType<T>() == "uint32_t")
    {
        metadata->var_type = UINT32;
        metadata->sizeof_var_type = sizeof(uint32_t);
        // metadata->min_value.u_integer_32 = variable.Min();
        std::cout << "parse variable metadata"
                  << "sizeof var type " << metadata->sizeof_var_type
                  << std::endl;
    }
    else if (helper::GetType<T>() == "int64_t")
    {
        metadata->var_type = INT64;
        metadata->sizeof_var_type = sizeof(int64_t);
        std::cout << "parse variable metadata"
                  << "sizeof var type " << metadata->sizeof_var_type
                  << std::endl;
    }
    else if (helper::GetType<T>() == "uint64_t")
    {
        metadata->var_type = UINT64;
        metadata->sizeof_var_type = sizeof(uint64_t);
    }
    else if (helper::GetType<T>() == "float")
    {
        metadata->var_type = FLOAT;
        metadata->sizeof_var_type = sizeof(float);
        std::cout << "parse variable metadata"
                  << "sizeof var type " << metadata->sizeof_var_type
                  << std::endl;
    }
    else if (helper::GetType<T>() == "double")
    {
        metadata->var_type = DOUBLE;
        metadata->sizeof_var_type = sizeof(double);
    }
    else if (helper::GetType<T>() == "long double")
    {
        metadata->var_type = LONG_DOUBLE;
        metadata->sizeof_var_type = sizeof(long double);
    }

    else if (helper::GetType<T>() == "float complex")
    {
        metadata->var_type = COMPLEX_FLOAT;
        // metadata->sizeof_var_type = sizeof(float complex); //TODO
    }
    else if (helper::GetType<T>() == "double complex")
    {
        metadata->var_type = COMPLEX_DOUBLE;
        // metadata->sizeof_var_type = sizeof(double complex); //TODO
    }
}

template <>
void parse_variable_type<std::string>(Variable<std::string> &variable, const std::string *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = STRING;
        metadata->sizeof_var_type = sizeof(std::string);
        // metadata->min_value->string = variable.Min().;
        std::cout << "parse_variable_type int8_t: min = " << variable.Min() << std::endl;
}


template <>
void parse_variable_type<int8_t>(Variable<int8_t> &variable, const int8_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = INT8;
        metadata->sizeof_var_type = sizeof(int8_t);
        metadata->min_value.integer_8 = variable.Min();
        std::cout << "parse_variable_type int8_t: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<uint8_t>(Variable<uint8_t> &variable, const uint8_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = UINT8;
        metadata->sizeof_var_type = sizeof(uint8_t);
        metadata->min_value.u_integer_8 = variable.Min();
        std::cout << "parse_variable_type uint8_t: min = " << variable.Min() << std::endl;
}


template <>
void parse_variable_type<int16_t>(Variable<int16_t> &variable, const int16_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = INT16;
        metadata->sizeof_var_type = sizeof(int16_t);
        metadata->min_value.integer_16 = variable.Min();
        std::cout << "parse_variable_type int16_t: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<uint16_t>(Variable<uint16_t> &variable, const uint16_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = UINT16;
        metadata->sizeof_var_type = sizeof(uint16_t);
        metadata->min_value.u_integer_16 = variable.Min();
        std::cout << "parse_variable_type uint16_t: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<int32_t>(Variable<int32_t> &variable, const int32_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = INT32;
        metadata->sizeof_var_type = sizeof(int32_t);
        metadata->min_value.integer_32 = variable.Min();
        std::cout << "parse_variable_type int32_t: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<uint32_t>(Variable<uint32_t> &variable, const uint32_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = UINT32;
        metadata->sizeof_var_type = sizeof(uint32_t);
        metadata->min_value.u_integer_32 = variable.Min();
        std::cout << "parse_variable_type uint32_t: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<int64_t>(Variable<int64_t> &variable, const int64_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = INT64;
        metadata->sizeof_var_type = sizeof(int64_t);
        metadata->min_value.integer_64 = variable.Min();
        std::cout << "parse_variable_type int64_t: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<uint64_t>(Variable<uint64_t> &variable, const uint64_t *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.1: " << std::endl;
        metadata->var_type = UINT64;
        metadata->sizeof_var_type = sizeof(uint64_t);
        metadata->min_value.u_integer_64 = variable.Min();
        std::cout << "parse_variable_type uint64_t: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<float>(Variable<float> &variable, const float *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.2: " << std::endl;
        metadata->var_type = UINT8;
        metadata->sizeof_var_type = sizeof(uint8_t);
        metadata->min_value.real_float = variable.Min();
        std::cout << "parse_variable_type float: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<double>(Variable<double> &variable, const double *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.2: " << std::endl;
        metadata->var_type = DOUBLE;
        metadata->sizeof_var_type = sizeof(double);
        metadata->min_value.real_double = variable.Min();
        std::cout << "parse_variable_type double: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<long double>(Variable<long double> &variable, const long double *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.2: " << std::endl;
        metadata->var_type = LONG_DOUBLE;
        metadata->sizeof_var_type = sizeof(long double);
        metadata->min_value.real_long_double = variable.Min();
        std::cout << "parse_variable_type long double: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<std::complex<float>>(Variable<std::complex<float>> &variable, const std::complex<float> *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.2: " << std::endl;
        metadata->var_type = COMPLEX_FLOAT;
        metadata->sizeof_var_type = sizeof(std::complex<float>);
        metadata->min_value.complex_float = variable.Min();
        std::cout << "parse_variable_type std::complex<float>: min = " << variable.Min() << std::endl;
}

template <>
void parse_variable_type<std::complex<double>>(Variable<std::complex<double>> &variable, const std::complex<double> *data,
                         Metadata *metadata)
{
        std::cout << "++ Julea Writer DEBUG PRINT 1.2: " << std::endl;
        metadata->var_type = COMPLEX_DOUBLE;
        metadata->sizeof_var_type = sizeof(std::complex<double>);
        metadata->min_value.complex_double = variable.Min();
        std::cout << "parse_variable_type std::complex<double>: min = " << variable.Min() << std::endl;
}


} // end namespace engine
} // end namespace core
} // end namespace adios2
#endif /* ADIOS_ENGINE_JULEAFORMATWRITER_ */
