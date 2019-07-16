/*
 * Distributed under the OSI-approved Apache License, Version 2.0.  See
 * accompanying file Copyright.txt for details.
 *
 * JULEA engine using the JULEA storage framework to handle lower I/O.
 *
 *  Created on: Nov 14, 2018
 *      Author: Kira Duwe duwe@informatik.uni-hamburg.de
 */

#ifndef ADIOS2_ENGINE_JULEACLIENTLOGIC_H_
#define ADIOS2_ENGINE_JULEACLIENTLOGIC_H_

#include "JuleaMetadata.h"
// #include "adios2/../julea/include/julea.h" KILLME!
#include <julea.h>
// #include <julea-adios.h>


namespace adios2
{
namespace core
{
namespace engine
{


// void j_adios_init(JuleaInfo* julea_info); //DESIGN: param needed?
// void j_adios_finish(void);

/* performs data put AND metadata put*/
void PutVariableToJulea(char* name_space, Metadata* metadata, void* data_pointer, JBatch* batch);
void PutAttributeToJulea(char* name_space, AttributeMetadata* attr_metadata, void* data_pointer, JBatch* batch);
// void j_adios_put_variable(char* name_space, Metadata* metadata, void* data_pointer, JBatch* batch, gboolean use_batch);
// void j_adios_put_attribute(char* name_space, AttributeMetadata* attr_metadata, void* data_pointer, JBatch* batch, gboolean use_batch);

/* get data from object store*/
void j_adios_get_var_data(char* name_space, char* variable_name, unsigned int length, void* data_pointer, JBatch* batch, gboolean use_batch);
void j_adios_get_attr_data(char* name_space, char* attribute_name, unsigned int length, void* data_pointer, JBatch* batch, gboolean use_batch);

/* get metadata from kv store; hopefully soon from SMD backend*/
void j_adios_get_all_var_names_from_kv(char* name_space, char*** names, int** types, unsigned int* count_names, JSemantics* semantics);
void j_adios_get_var_metadata_from_kv(char* name_space, char* var_name, Metadata* metadata, JSemantics* semantics);

/* get attributes from kv store; hopefully soon from SMD backend */
void j_adios_get_all_attr_names_from_kv(char* name_space, char*** names, int** types, unsigned int count_names, JSemantics* semantics);
void j_adios_get_attr_metadata_from_kv(char* name_space, char* var_name, AttributeMetadata* attr_metadata, JSemantics* semantics);

void j_adios_delete_variable(char* name_space, char* var_name, JBatch* batch);
void j_adios_delete_attribute(char* name_space, char* attr_name, JBatch* batch);

} // end namespace engine
} // end namespace core
} // end namespace adios2

#endif /* ADIOS2_ENGINE_JULEAMPIWRITER_H_ */
