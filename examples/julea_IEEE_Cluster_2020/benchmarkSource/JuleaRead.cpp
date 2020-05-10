/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * An application demonstrating some of the query possibilities enabled by the
 * JULEA database engine.
 *
 *  Created on: May 06, 2020
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */
#include <adios2.h>
#include <iomanip>
#include <iostream>
#include <vector>

#include <julea-db.h>
#include <julea.h>

void setMinMaxString(const char *varType, std::string &minField,
                     std::string &maxField, std::string &valueField)
{
    if ((strcmp(varType, "char") == 0) || (strcmp(varType, "int8_t") == 0) ||
        (strcmp(varType, "uint8_t") == 0) ||
        (strcmp(varType, "int16_t") == 0) ||
        (strcmp(varType, "uint16_t") == 0) || (strcmp(varType, "int32_t") == 0))
    {
        minField = "min-sint32";
        maxField = "max-sint32";
        valueField = "value-sint32";
    }
    else if (strcmp(varType, "uint32_t") == 0)
    {
        minField = "min-uint32";
        maxField = "max-uint32";
        valueField = "value-uint32";
    }
    else if (strcmp(varType, "int64_t") == 0)
    {
        minField = "min-sint64";
        maxField = "max-sint64";
        valueField = "value-sint64";
    }
    else if (strcmp(varType, "uint64_t") == 0)
    {
        minField = "min-uint64";
        maxField = "max-uint64";
        valueField = "value-uint64";
    }
    else if (strcmp(varType, "float") == 0)
    {
        minField = "min-float32";
        maxField = "max-float32";
        valueField = "value-float32";
    }
    else if (strcmp(varType, "double") == 0)
    {
        minField = "min-float64";
        maxField = "max-float64";
        valueField = "value-float64";
    }
    else if (strcmp(varType, "string") == 0)
    {
        valueField = "value-sint32";
    }

    else if ((strcmp(varType, "long double") == 0) ||
             (strcmp(varType, "float complex") == 0) ||
             (strcmp(varType, "double complex") == 0))
    {
        minField = "min-blob";
        maxField = "max-blob";
        valueField = "value-blob";
    }
}

void JuleaRead(std::string engineName, std::string directory, size_t fileCount, uint32_t percentageVarsToRead)
{
    std::cout << "JuleaRead" << std::endl;
}

void JuleaReadMinMax(std::string fileName, std::string variableName)
{
    size_t err = 0;
    size_t db_length = 0;
    gchar *db_field = NULL;
    JDBType type;
    JDBSchema *schema = NULL;
    JDBEntry *entry = NULL;
    JDBIterator *iterator = NULL;
    JDBSelector *selector = NULL;

    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);

    // schema = j_db_schema_new("adios2", "variable-metadata", NULL);
    schema = j_db_schema_new("adios2", "variable-blockmetadata", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);

    selector = j_db_selector_new(schema, J_DB_SELECTOR_MODE_AND, NULL);
    j_db_selector_add_field(selector, "file", J_DB_SELECTOR_OPERATOR_EQ,
                            fileName.c_str(), strlen(fileName.c_str()) + 1,
                            NULL);
    j_db_selector_add_field(selector, "variableName", J_DB_SELECTOR_OPERATOR_EQ,
                            variableName.c_str(),
                            strlen(variableName.c_str()) + 1, NULL);
    iterator = j_db_iterator_new(schema, selector, NULL);

    std::string adiosType = "double";
    std::string minField;
    std::string maxField;
    std::string valueField;
    setMinMaxString(adiosType.c_str(), minField, maxField, valueField);

    std::cout << "minField: " << minField << std::endl;
    std::cout << "adiosType: " << adiosType << std::endl;

    if (adiosType == "compound")
    {
    }
#define declare_type(T)                                                        \
    else if (adiosType == adios2::GetType<T>())                                \
    {                                                                          \
        T *min;                                                                \
        T *max;                                                                \
        while (j_db_iterator_next(iterator, NULL))                             \
        {                                                                      \
            j_db_iterator_get_field(iterator, minField.c_str(), &type,         \
                                    (gpointer *)&min, &db_length, NULL);       \
            j_db_iterator_get_field(iterator, maxField.c_str(), &type,         \
                                    (gpointer *)&max, &db_length, NULL);       \
        }                                                                      \
    }
    ADIOS2_FOREACH_STDTYPE_1ARG(declare_type)
#undef declare_type
    // std::cout << "min: " << min << std::endl;                          \
            // std::cout << "max: " << max << std::endl;                          \

    j_db_schema_unref(schema);
    // j_db_iterator_unref(iterator);
    j_db_selector_unref(selector);
    // j_batch_unref(batch);
    // j_db_entry_unref(entry);
}

