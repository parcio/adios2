#ifndef NETCDF_READ_H
#define NETCDF_READ_H

void NCReadFile(std::string engine, std::string ncFileName, std::string adiosFileName,
          bool printDimensions, bool printVariable);
#endif /* NETCDF_READ_H */
