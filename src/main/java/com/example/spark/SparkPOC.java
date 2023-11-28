package com.example.spark;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.net.URI;

@Slf4j
public class SparkPOC {
    private static final String HDFS = "hdfs://host.docker.internal:8020";

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("spark://localhost:7077")
                .appName("SparkPOC")
                .config("spark.driver.port", "8888")
                .config("spark.driver.bindAddress", "0.0.0.0")
                .getOrCreate();
        fileSystemCopyFromLocalFile();
        readAndWriteInParquet(spark);
        spark.close();
    }

    private static void readAndWriteInParquet(SparkSession spark) {
        spark.read()
                .json(HDFS + "/deals.json")
                .write()
                .mode(SaveMode.Append)
                .parquet(HDFS + "/deals-parquet.parquet");
    }

    @SneakyThrows
    private static void fileSystemCopyFromLocalFile() {
        FileSystem fs = factoryFileSystem();
        fs.copyFromLocalFile(
                new Path("C:\\Users\\David\\Music\\deals.json"),
                new Path("/deals.json")
        );
        fs.close();
    }

    @SneakyThrows
    private static FileSystem factoryFileSystem() {
        Configuration configuration = new Configuration();
        configuration.setBoolean("dfs.client.use.datanode.hostname", true);
        return FileSystem.get(new URI(HDFS), configuration);
    }
}
