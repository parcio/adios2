#ifndef NETCDF_READ_H
#define NETCDF_READ_H
#include <iostream>
void NCReadFile(std::string engine, std::string ncFileName, std::string adiosFileName,
          bool printDimensions, bool printVariable, bool needsTransform);
#endif /* NETCDF_READ_H */
