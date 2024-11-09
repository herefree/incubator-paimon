package org.apache.paimon.spark;

import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.TestHiveMetastore;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

public class SparkViewTest {
    private static TestHiveMetastore testHiveMetastore;

    private static final int PORT = 9087;

    @BeforeAll
    public static void startMetastore() {
        testHiveMetastore = new TestHiveMetastore();
        testHiveMetastore.start(PORT);
    }

    @AfterAll
    public static void closeMetastore() throws Exception {
        testHiveMetastore.stop();
    }

//    @Test
//    public void testView(@TempDir java.nio.file.Path tempDir) {
//        // firstly, we use hive metastore to create table, and check the result.
//        Path warehousePath = new Path("file:" + tempDir.toString());
//        SparkSession spark =
//                SparkSession.builder()
//                        .config("spark.sql.warehouse.dir", warehousePath.toString())
//                        // with case-sensitive false
//                        .config("spark.sql.caseSensitive", "false")
//                        // with hive metastore
//                        .config("spark.sql.catalogImplementation", "hive")
//                        .config(
//                                "spark.sql.catalog.spark_catalog",
//                                SparkCatalog.class.getName())
//                        .master("local[2]")
//                        .getOrCreate();
//
//        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
//        spark.sql("USE my_db1");
//        spark.sql(
//                "CREATE TABLE IF NOT EXISTS t2 (a INT, Bb INT, c STRING) USING paimon TBLPROPERTIES"
//                        + " ('file.format'='avro')");
//
//        spark.sql("CREATE DATABASE IF NOT EXISTS my_db1");
//        spark.sql("USE spark_catalog.my_db1");
//        spark.sql(
//                "CREATE TABLE db_pt (a INT, b INT, c STRING) USING paimon TBLPROPERTIES"
//                        + " ('file.format'='avro')");
//        spark.sql("INSERT INTO db_pt VALUES (1, 2, '3'), (4, 5, '6')");
//        spark.sql("CREATE VIEW spark_view as select a , b from db_pt");
//        spark.sql("show views").collectAsList().forEach(System.out::println);
//        spark.sql("select * from spark_view").collectAsList().forEach(System.out::println);
//
//
//        spark.close();
//    }
}
