#ifndef ADIOS2_READ_H_
#define ADIOS2_READ_H_


void AdiosReadMinMax(std::string fileName, std::string variableName);
void AdiosRead(std::string engineName, std::string directory, size_t fileCount, uint32_t percentageVarsToRead);

#endif /* ADIOS2_READ_H_ */
