-- -KV-- -

    // original
    void PutNameToJulea(std::string paramName, std::string nameSpace,
                        std::string kvName);
void PutVariableMetadataToJulea(const std::string nameSpace, gpointer buffer,
                                guint32 bufferLen, const std::string varName);
void PutBlockMetadataToJulea(const std::string nameSpace,
                             const std::string varName, gpointer &buffer,
                             guint32 bufferLen, const std::string stepBlockID);
template <class T>
void PutVariableDataToJulea(Variable<T> &variable, const T *data,
                            const std::string nameSpace, size_t currentStep,
                            size_t blockID);

// rename?
void PutNameToKV(std::string paramName, std::string nameSpace,
                 std::string kvName);
void PutVariableMetadataToKV(const std::string nameSpace, gpointer buffer,
                             guint32 bufferLen, const std::string varName);
void PutBlockMetadataToKV(const std::string nameSpace,
                          const std::string varName, gpointer &buffer,
                          guint32 bufferLen, const std::string stepBlockID);
template <class T>
void PutVariableDataToKV(Variable<T> &variable, const T *data,
                         const std::string nameSpace, size_t currentStep,
                         size_t blockID);

-- -DB-- -

    /** --- Variables --- */
    void InitDBSchemas();
template <class T>
void DBPutVariableMetadataToJulea(Variable<T> &variable,
                                  const std::string nameSpace,
                                  const std::string varName, size_t currStep,
                                  size_t block);
template <class T>
void DBPutBlockMetadataToJulea(Variable<T> &variable,
                               const std::string nameSpace,
                               const std::string varName, size_t step,
                               size_t block,
                               const typename Variable<T>::Info &blockInfo,
                               T &blockMin, T &blockMax, T &blockMean,
                               uint32_t &entryID);
template <class T>
void DBPutVariableDataToJulea(Variable<T> &variable, const T *data,
                              const std::string nameSpace, uint32_t entryID);