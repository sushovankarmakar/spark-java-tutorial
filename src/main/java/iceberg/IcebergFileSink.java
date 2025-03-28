//package iceberg;
//
//import in.airtel.di.exceptions.GenericException;
//import in.airtel.di.generatedModels.yaml.model.*;
//import in.airtel.di.util.SparkContextHolder;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.iceberg.*;
//import org.apache.iceberg.actions.RewriteDataFiles;
//import org.apache.iceberg.catalog.Catalog;
//import org.apache.iceberg.catalog.TableIdentifier;
//import org.apache.iceberg.expressions.Expressions;
//import org.apache.iceberg.expressions.UnboundTerm;
//import org.apache.iceberg.hive.HiveCatalog;
//import org.apache.iceberg.spark.Spark3Util;
//import org.apache.iceberg.spark.SparkSchemaUtil;
//import org.apache.iceberg.spark.actions.SparkActions;
//import org.apache.log4j.Logger;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.apache.spark.sql.connector.expressions.Transform;
//import org.apache.spark.sql.types.StructType;
//import org.jetbrains.annotations.NotNull;
//
//import java.io.IOException;
//import java.time.LocalDateTime;
//import java.time.format.DateTimeFormatter;
//import java.time.format.DateTimeFormatterBuilder;
//import java.time.temporal.ChronoField;
//import java.time.temporal.ChronoUnit;
//import java.util.*;
//import java.util.stream.Collectors;
//
//import static in.airtel.di.exceptions.IngestionExceptions.INVALID_SAVEMODE_EXCEPTION;
//import static org.apache.iceberg.TableProperties.*;
//
//public class IcebergFileSink implements ISink {
//
//    SparkSession sparkSession;
//    Connection connection;
//    Sink sink;
//    private static boolean isCaseSensitive;             //defaults to false, fetched from source
//
//    private static final Logger LOG = Logger.getLogger(IcebergFileSink.class);
//
//    public IcebergFileSink(SparkSession sparkSession, Connection connection, Sink sink){
//        this.connection = connection;
//        this.sparkSession = sparkSession;
//        this.sink = sink;
//    }
//    private static final Set<String> REQUIRED_KEYS = createSet("format", "path", "mode");
//    private static final Set<String> VALID_FORMATS = createSet("parquet", "orc");
//    private static final Set<String> VALID_SORT_SCOPES = createSet("local", "global");
//    private static final Set<String> VALID_MODES = createSet("append", "overwrite", "insert overwrite");
//    private static final Set<String> VALID_DIST_MODES = createSet("none", "hash", "range");
//
//    public void write(Dataset<Row> inputDataset){
//        // checking partitionSpec
//        // get older partitionSpec
//        // we have to see if there is change in partitionSpec, for that we need to create partitionSpec from the string info
//        // Now if we see there is difference between partitionSpec, it could mean change of order, add or remove of partition columns
//        // If different, i need to have a method which may have to remove all existing fields from partition spec and add new ones.
//        // But if there's only a addition at the end or removal of a partition col, it may seem better to just add or remove that field
//        //can you give me a modular java code for this
//
//        HiveContext hiveContext = this.sink.getContextProperties().getHiveContext();
//        ConnectionContext connectionContext = this.connection.getContextProperties().getConnectionContext();
//        StoreContext storeContext1 = this.sink.getContextProperties().getStoreContext();
//
//        //set spark configurations
//
//        String database = hiveContext.getAdditionalProperties().get("db");
//        String tableName = hiveContext.getAdditionalProperties().get("tableName");
//        String nameService = connectionContext.getAdditionalProperties().get("uri");
//
//        Map<String, String> storeContext = storeContext1.getAdditionalProperties();
//        String basePath = nameService + "/" + storeContext.get("path");
//        validateSinkProperties(storeContext);
//
//        writeIcebergTable(sparkSession, inputDataset, database, tableName, basePath, storeContext);
//
//    }
//
//    public static void writeIcebergTable(SparkSession sparkSession, Dataset<Row> inputDataset, String database, String tableName, String basePath, Map<String, String> storeContext) {
//        setSparkSessionConfigs(sparkSession);
//        HashMap<String, String> catalogProperties = new HashMap<>();
//        catalogProperties.put("metrics-reporter-impl","org.apache.iceberg.metrics.LoggingMetricsReporter") ;
//
//        Catalog catalog = CatalogUtil.loadCatalog(HiveCatalog.class.getName(), "spark_catalog", catalogProperties, sparkSession.sparkContext().hadoopConfiguration());
//
//        HashMap<String, String> tableProperties = getTableProperties(storeContext);
//        boolean exists =  checkTableExists(sparkSession, database, tableName,basePath);
//        if(!exists){
//            createIcebergTable(catalog, inputDataset.schema(),tableProperties,basePath, database, tableName);
//        }
//        String partitionString = storeContext.getOrDefault("partitionBy","");
//        String sortOrderString = storeContext.getOrDefault("sortBy","");
//        String sortOrderScope = storeContext.getOrDefault("sortScope","");
//        String distMode = storeContext.get("distMode");
//        long targetFileSize = Long.parseLong(storeContext.getOrDefault("targetFileSize","536870912"));
//        String minFileSize = storeContext.getOrDefault("minFileSize",Long.toString((long)(targetFileSize*0.75)));
//        String maxFileSize = storeContext.getOrDefault("maxFileSize",Long.toString((long)(targetFileSize*1.80)));
//        String rewriteAll = storeContext.getOrDefault("rewriteAll","false"); //overrides all other checks and mandatorily does compaction
//        String minInputFiles = storeContext.getOrDefault("minInputFiles","4");
//
//        int compactionFrequency = Integer.parseInt(storeContext.getOrDefault("compactionFrequency","24"));
//        updatePartitionSpecAndSortOrder(catalog,database,tableName,partitionString,sortOrderString, sortOrderScope,distMode);
//        inputDataset.createOrReplaceTempView("sink_output");
//
//        String mode = storeContext.getOrDefault("mode","append");
//        if(mode.equalsIgnoreCase("overwrite")){
//            sparkSession.sql("explain insert overwrite "+ database +"."+ tableName +" select * from sink_output").show(false);
//            sparkSession.sql("insert overwrite "+ database +"."+ tableName +" select * from sink_output");
//        } else if (mode.equalsIgnoreCase("append")) {
//            sparkSession.sql("explain insert into "+ database +"."+ tableName +" select * from sink_output").show(false);
//            sparkSession.sql("insert into "+ database +"."+ tableName +" select * from sink_output");
//
//        }else{
//            throw new GenericException(INVALID_SAVEMODE_EXCEPTION,"Invalid mode: "+mode);
//        }
//        //check Last compaction time
//        // if more than frequency, then run inline compaction
//
//        runCompaction(sparkSession,catalog, database, tableName, targetFileSize, minFileSize, maxFileSize, rewriteAll, compactionFrequency,minInputFiles);
//    }
//
//    private static void runCompaction(SparkSession sparkSession,Catalog catalog, String database, String tableName, Long targetFileSize, String minFileSize, String maxFileSize, String rewriteAll, int compactionFrequency,String minInputFiles) {
//        try {
//            final List<Row> list = sparkSession.sql("select cast(committed_at as string) from " + database + "." + tableName + ".snapshots" +  " where operation = 'replace' order by committed_at desc limit 1").collectAsList();
//
//            TableIdentifier tableIdentifier = TableIdentifier.of(database, tableName);
//            Table table = catalog.loadTable(tableIdentifier);
//            int maxConcurrentFileGroupRewrites = 10; // Max concurrent rewrites
//            String jobOrder = "files-desc";      // Rewrite job order
//            boolean useStartingSequenceNumber = true; // Use starting sequence number
//            boolean runCompaction = false;
//
//            if (list.size() > 0) {
//                //check if compaction is required
//                DateTimeFormatter formatter = new DateTimeFormatterBuilder()
//                        .appendPattern("yyyy-MM-dd HH:mm:ss")       // Base date and time format
//                        .optionalStart()                            // Start optional section
//                        .appendFraction(ChronoField.MICRO_OF_SECOND, 1, 9, true)  // Fractional seconds, 1 to 9 digits
//                        .optionalEnd()                              // End optional section
//                        .toFormatter();
//                LocalDateTime latestCompactionTime = LocalDateTime.parse(list.get(0).getString(0), formatter);
//                LOG.info("Last Compaction time is :" + latestCompactionTime);
//
//                LocalDateTime currentTime = LocalDateTime.now();
//                long hoursDifference = ChronoUnit.HOURS.between(latestCompactionTime, currentTime);
//                LOG.info("Hours difference: " + hoursDifference);
//                if (hoursDifference >= compactionFrequency) {
//                    runCompaction = true;
//                }
//            }else{
//                runCompaction = true;
//            }
//            if(runCompaction){
//                LOG.info("Running compaction");
//                final RewriteDataFiles.Result result = SparkActions.get().rewriteDataFiles(table)
//                        .option("min-file-size-bytes", String.valueOf(minFileSize))
//                        .option("max-file-size-bytes", String.valueOf(maxFileSize))
//                        .option("target-file-size-bytes", String.valueOf(targetFileSize))
//                        .option("min-input-files", String.valueOf(minInputFiles))
//                        .option("rewrite-all", String.valueOf(rewriteAll))
//                        .option("max-concurrent-file-group-rewrites", String.valueOf(maxConcurrentFileGroupRewrites))
//                        .option("rewrite-job-order", jobOrder)
//                        .option("use-starting-sequence-number", String.valueOf(useStartingSequenceNumber)).execute();
//
//                LOG.info("Compaction Completed");
//                LOG.info("Compaction Result:");
//                LOG.info("Files rewritten: " + result.rewrittenDataFilesCount());
//                LOG.info("Files added: " + result.addedDataFilesCount());
//                LOG.info("Bytes rewritten: " + result.rewrittenBytesCount());
//            }
//        } catch (Exception e){
//            LOG.error("Error while running compaction: "+e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    public static void setSparkSessionConfigs(SparkSession sparkSession) {
//        sparkSession.conf().set("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog");
//        sparkSession.conf().set("spark.sql.catalog.spark_catalog.type","hive");
//        sparkSession.conf().set("spark.sql.sources.partitionOverwriteMode","static");
//        LOG.info("Added spark configurations successfully");
//    }
//
//    @NotNull
//    private static HashMap<String, String> getTableProperties(Map<String, String> storeContext) {
//        String format = storeContext.getOrDefault("format","parquet");
//        HashMap<String, String> tableProperties = new HashMap<>();
//        tableProperties.put("write.format.default", format);
//        tableProperties.put("write.spark.fanout.enabled", "true");
//        tableProperties.put("write.parquet.compression-codec", "zstd");
//        tableProperties.put("write.target-file-size-bytes", storeContext.getOrDefault("targetFileSize","536870912"));
//        tableProperties.put("mergeSchema","true");
//        tableProperties.put("use-table-distribution-and-ordering","true");
//        tableProperties.put("write.spark.accept-any-schema", "true");
//        storeContext.forEach((k, v) -> {
//            if(!(k.equalsIgnoreCase("path") || k.equalsIgnoreCase("format")
//                    || k.equalsIgnoreCase("partitionBy") || k.equalsIgnoreCase("sortBy")
//                    || k.equalsIgnoreCase("sortScope"))){
//                tableProperties.put(k,v);
//            }
//
//        });
//        return tableProperties;
//    }
//
//
//    private static boolean checkTableExists(SparkSession sparkSession, String database, String tableName, String basePath) {
//        boolean pathExists = true;
//        boolean tableExists = true;
//        try {
//            final FileSystem fileSystem = new Path(basePath).getFileSystem(sparkSession.sparkContext().hadoopConfiguration());
//            if(!fileSystem.exists( new Path(basePath))){
//                pathExists = false;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        if(!sparkSession.catalog().tableExists(database, tableName)){
//            LOG.info("Iceberg table not found: "+tableName);
//            tableExists = false;
//        }
//        if(!pathExists && !tableExists) {
//            return false;
//        } else if (pathExists && tableExists) {
//            return true;
//
//        } else {
//            LOG.error("Table exists: " + tableExists);
//            LOG.error("Path exists: " + pathExists);
//            throw new RuntimeException("Table exists: " + tableExists + " Path exists: " + pathExists);
//        }
//    }
//
//
//    public static boolean createIcebergTable(Catalog catalog,StructType schema,HashMap<String,String> tableProperties,String basePath, String database, String tableName) {
//        Schema icebergSchema = SparkSchemaUtil.convert(schema);
//        PartitionSpec partitionSpec = PartitionSpec.builderFor(icebergSchema).build();
//        TableIdentifier tableIdentifier = TableIdentifier.of(database, tableName);
//        Table table = catalog.createTable(tableIdentifier, icebergSchema, partitionSpec, basePath, tableProperties);
//        return true;
//    }
//
//
//
//    public static void updatePartitionSpecAndSortOrder(Catalog catalog, String dbName, String tableName,String partitionString, String sortOrderString, String sortOrderScope,String distMode) {
//        // Load the Iceberg table
//        // Build the new partition spec
//        // Retrieve the current partition specification and sort order
//        // Check if the new partition spec is different from the current one
//        // Update partition spec if different
//        // Start and commit a transaction
//
//        TableIdentifier tableIdentifier = TableIdentifier.of(dbName, tableName);
//        Table table = catalog.loadTable(tableIdentifier);
//        final Schema schema = table.schema();
//        PartitionSpec newSpec = buildPartitionSpec(partitionString, schema);
//        PartitionSpec currentSpec = table.spec();
//        SortOrder currentSortOrder = table.sortOrder();
//        Transaction transaction = table.newTransaction();
//
//        if (isDifferentPartitionSpec(currentSpec,newSpec)) {
//            updatePartitionSpec(newSpec,currentSpec, transaction);
//
//        }
//        updateSortOrder(transaction, sortOrderString,sortOrderScope, newSpec.isPartitioned(),distMode);
//
//        transaction.commitTransaction();
//    }
//
//    public static PartitionSpec buildPartitionSpec(String partitionString, Schema schema) {
//        PartitionSpec.Builder specBuilder = PartitionSpec.builderFor(schema);
//
//        if(partitionString == null || partitionString.isEmpty()){
//            return PartitionSpec.unpartitioned();
//        }
//        for (String part : partitionString.split(",")) {
//            part = part.trim();
//            if(!isCaseSensitive){
//                part = part.toLowerCase();
//            }
//            if(!part.isEmpty()) {
//                if (part.startsWith("year(")) {
//                    String column = part.substring(5, part.length() - 1);
//                    specBuilder.year(column);
//                } else if (part.startsWith("month(")) {
//                    String column = part.substring(6, part.length() - 1);
//                    specBuilder.month(column);
//                } else if (part.startsWith("day(")) {
//                    String column = part.substring(4, part.length() - 1);
//                    specBuilder.day(column);
//                } else if (part.startsWith("hour(")) {
//                    String column = part.substring(5, part.length() - 1);
//                    specBuilder.hour(column);
//                } else {
//                    specBuilder.identity(part);
//                }
//            }
//        }
//        PartitionSpec spec = specBuilder.build();
//        return spec;
//    }
//
//    private static boolean isDifferentPartitionSpec(PartitionSpec currentSpec, PartitionSpec newSpec) {
//        List<PartitionField> currentFields = currentSpec.fields();
//        List<PartitionField> newFields = newSpec.fields();
//
//        if (currentFields.size() != newFields.size()) {
//            return true;
//        }
//
//        for (int i = 0; i < currentFields.size(); i++) {
//            PartitionField currentField = currentFields.get(i);
//            PartitionField newField = newFields.get(i);
//
//            // Check if the field name and transform are the same
//            if (!currentField.name().equals(newField.name()) ||
//                    !currentField.transform().toString().equals(newField.transform().toString())) {
//                return true;
//            }
//        }
//        return false;
//    }
//
//    public static void updatePartitionSpec(PartitionSpec newSpec, PartitionSpec currentSpec, Transaction transaction) {
//        final Transform[] transforms = Spark3Util.toTransforms(newSpec);
//        int i = 0 ;
//        //Remove older PartitionFields
//        {
//            UpdatePartitionSpec updateSpec = transaction.updateSpec().caseSensitive(isCaseSensitive);
//            for (PartitionField field : currentSpec.fields()) {
//                updateSpec = updateSpec.removeField(field.name());
//            }
//            updateSpec.commit();
//        }
//        {
//            UpdatePartitionSpec updateSpec = transaction.updateSpec().caseSensitive(isCaseSensitive);
//            for (PartitionField field : newSpec.fields()) {
//                updateSpec = updateSpec.addField(field.name(), Spark3Util.toIcebergTerm(transforms[i]));
//                i++;
//            }
//            updateSpec.commit();
//        }
//        LOG.info("Partition spec updated successfully.");
//    }
//
//    public static void updateSortOrder(Transaction transaction,String sortOrderString, String sortOrderScope, boolean isPartitioned, String distMode) {
//        if(sortOrderString == null || sortOrderString.isEmpty()){
//            return;
//         }
//        ReplaceSortOrder newSortOrderBuilder = transaction.replaceSortOrder().caseSensitive(isCaseSensitive);
//         Arrays.stream(sortOrderString.split(",")).forEach(order -> {
//             String[] parts = order.split(" ");
//             String column = parts[0];
//             String direction = parts[1];
//             UnboundTerm term = Expressions.ref(column);
//             if (direction.equalsIgnoreCase("asc")) {
//                 newSortOrderBuilder.asc(term);
//             } else if (direction.equalsIgnoreCase("desc")) {
//                 newSortOrderBuilder.desc(term);
//
//             }});
//         newSortOrderBuilder.commit();
//         if(distMode!=null){
//             transaction.updateProperties().set(WRITE_DISTRIBUTION_MODE, distMode).commit();
//         }else {
//             if (sortOrderScope != null && sortOrderScope.equalsIgnoreCase("global")) {
//                 transaction.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_RANGE).commit();
//             } else if (sortOrderScope != null && sortOrderScope.equalsIgnoreCase("local")) {
//                 if (isPartitioned) {
//                     transaction.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit();
//                 } else {
//                     transaction.updateProperties().set(WRITE_DISTRIBUTION_MODE, WRITE_DISTRIBUTION_MODE_HASH).commit(); //to tackle small file issue
//                 }
//             }
//         }
//        LOG.info("Sort order updated successfully.");
//    }
//
//    @Override
//    public void setCaseSensitive(boolean caseSensitive) {
//        isCaseSensitive = caseSensitive;
//    }
//    private static Set<String> createSet(String... values) {
//        Set<String> set = new HashSet<>();
//        Collections.addAll(set, values);
//        return Collections.unmodifiableSet(set);
//    }
//    public static void validateSinkProperties(Map<String, String> properties) {
//        Set<String> missingKeys = REQUIRED_KEYS.stream()
//                .filter(key -> !properties.containsKey(key) || properties.get(key).isEmpty())
//                .collect(Collectors.toSet());
//
//        if (!missingKeys.isEmpty()) {
//            throw new IllegalArgumentException("Missing or empty required properties: " + String.join(", ", missingKeys));
//        }
//
//        if (!VALID_FORMATS.contains(properties.get("format").toLowerCase())) {
//            throw new IllegalArgumentException("Invalid format: " + properties.get("format"));
//        }
//        if(properties.containsKey("sortScope")) {
//            if (!VALID_SORT_SCOPES.contains(properties.get("sortScope").toLowerCase())) {
//                throw new IllegalArgumentException("Invalid sortScope: " + properties.get("sortScope"));
//            }
//        }
//
//        if (!VALID_MODES.contains(properties.get("mode").toLowerCase())) {
//            throw new IllegalArgumentException("Invalid mode: " + properties.get("mode"));
//        }
//        if (properties.get("distMode")!=null && !VALID_DIST_MODES.contains(properties.get("distMode").toLowerCase())) {
//            throw new IllegalArgumentException("Invalid distribution mode: " + properties.get("distMode"));
//        }
//    }
//
//
//    public static void main(String[] args) throws IOException {
//        SparkContextHolder sparkContextHolder = SparkContextHolder.getInstance();
//        SparkSession sparkSession = sparkContextHolder.getSparkSession();
////        sparkSession.sql("CALL spark_catalog.system.rewrite_data_files(table => 'iceberg_staging.iceberg_6nov_try5', options => map('max-concurrent-file-group-rewrites','10','rewrite-job-order','files-desc','use-starting-sequence-number' ,'true','min-file-size-bytes','402653184','max-file-size-bytes','966367641','target-file-size-bytes','536870912','min-input-files','4','rewrite-all','true'))").show(false);
//
//        Dataset<Row> dataset = sparkSession.read().format("orc").load("file:///Users/b0219039/Desktop/samples/optimus_aorc/part-01596-515037b3-0da8-4b08-8788-8f6f3a1c3d8e.c000.snappy.orc")
//                .selectExpr("*", "cast(from_unixtime(upd_dtm_inepoch/1000) as timestamp) as col_partition").limit(100);
//
//        final HashMap<String, String> storeContext = new HashMap<>();
//        final String tableName = "9july_test_6";
//        final String iceberg = "iceberg";
//        storeContext.put("format", "orc");
//        final String basePath = "hdfs://localhost:9000/user/b0219039/iceberg/" + tableName;
//        storeContext.put("path", basePath);
//
//
////        storeContext.put("partitionBy", "month(col_partition),cust_seg");
//        storeContext.put("sortBy", "si asc,upd_dtm_inepoch desc");
//        storeContext.put("sortScope", "local");
//        storeContext.put("mode", "overwrite");
//
//        writeIcebergTable(sparkSession, dataset, iceberg, tableName, basePath, storeContext);
//
//    }
//}
//
//
////Compaction Support Testing Pending
////GLOBAL or LOCAL order handling pending
