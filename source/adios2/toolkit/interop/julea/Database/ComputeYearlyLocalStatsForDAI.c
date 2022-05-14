DB_Init()
{

    yLocalSchema = j_db_schema_new("adios2", "yearly-local-statistics", NULL);

    j_db_schema_get(yLocalSchema, batch, NULL);
    bool existsYearlyLocal = j_batch_execute(batch);

    if (existsYearlyLocal == 0)
    {
        yLocalSchema =
            j_db_schema_new("adios2", "yearly-local-statistics", NULL);
        AddFieldsForYearlyLocalStatsTable(yLocalSchema);
        j_db_schema_create(yLocalSchema, batch4, NULL);
        g_assert_true(j_batch_execute(batch4) == true);
    }
}

/**
 * Table for the yearly min/max/mean/sum/var with "local" resolution
 * local resolution = yearly values at the block level
 * global stats: x/y set to -1/-1
 */
void JuleaDBInteractionWriter::AddFieldsForYearlyLocalStatsTable(
    JDBSchema *schema)
{
    gchar const *fileIndex[] = {"file", NULL};
    gchar const *varIndex[] = {"variableName", NULL};
    gchar const *x[] = {"x", NULL};
    gchar const *y[] = {"y", NULL};

    j_db_schema_add_field(schema, "file", J_DB_TYPE_STRING, NULL);
    j_db_schema_add_field(schema, "variableName", J_DB_TYPE_STRING, NULL);

    j_db_schema_add_field(schema, "year", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "x", J_DB_TYPE_SINT32, NULL);
    j_db_schema_add_field(schema, "y", J_DB_TYPE_SINT32, NULL);

    j_db_schema_add_field(schema, "yearly_blockMin", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "yearly_blockMax", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "yearly_blockMean", J_DB_TYPE_FLOAT64, NULL);
    j_db_schema_add_field(schema, "yearly_blockSum", J_DB_TYPE_FLOAT64, NULL);
    // j_db_schema_add_field(schema, "yearly_blockVar", J_DB_TYPE_FLOAT64,
    // NULL);

    j_db_schema_add_index(schema, fileIndex, NULL);
    j_db_schema_add_index(schema, varIndex, NULL);
    j_db_schema_add_index(schema, x, NULL);
    j_db_schema_add_index(schema, y, NULL);
}

void JuleaDBInteractionWriter::AddEntriesForYearlyLocalStatsTable(
    const std::string nameSpace, const std::string varName, size_t currentStep,
    interop::JuleaCDO &JuleaCDO, int writerRank)
{
    int err = 0;
    g_autoptr(JDBSchema) schema = NULL;
    g_autoptr(JDBEntry) entry = NULL;
    g_autoptr(JDBSelector) selector = NULL;
    g_autoptr(JDBIterator) iterator = NULL;
    JDBType jdbType;
    guint64 db_length = 0;
    uint32_t *tmpID;
    // size_t year = 0;
    // size_t month = 0;
    // size_t day = 0;
    // uint32_t entryID = 0;
    int x = 0;
    int y = 0;
    int year = 0;

    // TODO: find correct x/y coordinates
    JuleaCDO.ComputeCoordinatesFromRank(writerRank, x, y);
    if (currentStep >= JuleaCDO.m_StepsPerYear)
    {
        year = (size_t)currentStep / JuleaCDO.m_StepsPerYear;
    }

    JuleaCDO.ComputeYearlyLocalStats(varName);

    // void *namesBuf = NULL;
    auto semantics = j_semantics_new(J_SEMANTICS_TEMPLATE_DEFAULT);
    auto batch = j_batch_new(semantics);
    auto batch2 = j_batch_new(semantics);

    schema = j_db_schema_new("adios2", "yearly-local-statistics", NULL);
    j_db_schema_get(schema, batch, NULL);
    err = j_batch_execute(batch);
    // g_assert_true(j_batch_execute(batch) == true);

    entry = j_db_entry_new(schema, NULL);
    j_db_entry_set_field(entry, "file", nameSpace.c_str(),
                         strlen(nameSpace.c_str()) + 1, NULL);
    j_db_entry_set_field(entry, "variableName", varName.c_str(),
                         strlen(varName.c_str()) + 1, NULL);

    j_db_entry_set_field(entry, "year", &year, sizeof(year), NULL);
    j_db_entry_set_field(entry, "x", &x, sizeof(x), NULL);
    j_db_entry_set_field(entry, "y", &y, sizeof(y), NULL);

    // j_db_entry_set_field(entry, "yearly_blockMin", &variable,
    // sizeof(), NULL); j_db_entry_set_field(entry, "yearly_blockMax",
    // &variable, sizeof(), NULL); j_db_entry_set_field(entry,
    // "yearly_blockMean", &variable, sizeof(), NULL);
    // j_db_entry_set_field(entry, "yearly_blockSum", &variable,
    // sizeof(), NULL); j_db_entry_set_field(entry, "yearly_blockVar",
    // &variable, sizeof(), NULL);
}