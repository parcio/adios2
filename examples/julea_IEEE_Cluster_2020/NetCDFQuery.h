#ifndef NETCDF_QUERY_H
#define NETCDF_QUERY_H

void NCReadFile(std::string engine, std::string ncFileName, std::string adiosFileName,
          bool printDimensions, bool printVariable);
#endif /* NETCDF_QUERY_H */
