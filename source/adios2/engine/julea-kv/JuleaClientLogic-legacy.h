/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Jul 16, 2019
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEACLIENTLOGIC_H_
#define ADIOS2_ENGINE_JULEACLIENTLOGIC_H_

#include "JuleaKVWriter.h"
#include "JuleaMetadata.h"

// #include <julea.h>

namespace adios2
{
namespace core
{
namespace engine
{

// void j_adios_init(JuleaInfo* julea_info); //DESIGN: param needed?
// void j_adios_finish(void);

/* performs data put AND metadata put*/
void PutVariableToJulea(char *name_space, Metadata *metadata,
                        const void *data_pointer, JBatch *batch);
void PutAttributeToJulea(char *name_space, AttributeMetadata *attr_metadata,
                         void *data_pointer, JBatch *batch);

/* get data from object store*/
void GetVarDataFromJulea(char *name_space, char *variable_name,
                         unsigned int length, void *data_pointer,
                         JBatch *batch);
void GetAttrDataFromJulea(char *name_space, char *attribute_name,
                          unsigned int length, void *data_pointer,
                          JBatch *batch);

/* get metadata from kv store; hopefully soon from SMD backend*/
void GetAllVarNamesFromKV(char *name_space, char ***names, int **types,
                          unsigned int *count_names, JSemantics *semantics);
void GetVarMetadataFromKV(char *name_space, char *var_name, Metadata *metadata,
                          JSemantics *semantics);

/* get attributes from kv store; hopefully soon from SMD backend */
void GetAllAttrNamesFromKV(char *name_space, char ***names, int **types,
                           unsigned int *count_names, JSemantics *semantics);
void GetAttrMetadataFromKV(char *name_space, char *var_name,
                           AttributeMetadata *attr_metadata,
                           JSemantics *semantics);

/* delete variable data as well as metadata */
void DeleteVariable(char *name_space, char *var_name, JBatch *batch);
void DeleteAttribute(char *name_space, char *attr_name, JBatch *batch);

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEACLIENTLOGIC_H_ */
