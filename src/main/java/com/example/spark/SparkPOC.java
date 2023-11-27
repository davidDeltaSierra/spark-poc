package com.example.spark;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.net.URI;

import static org.apache.spark.sql.functions.col;

@Slf4j
public class SparkPOC {
    private static final String HDFS = "hdfs://host.docker.internal:8020";

    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
                .master("spark://localhost:7077")
                .appName("Simple Application 3")
                .config("spark.driver.port", "8888")
                .config("spark.driver.bindAddress", "0.0.0.0")
                .getOrCreate();

        //uploadAndConvertToParquet(spark);
        spark.read()
                .parquet(HDFS + "/deals-parquet.parquet")
                .select("id", "description")
                .where(col("id").equalTo(886224))
                .show(50);
    }

    private static void uploadAndConvertToParquet(SparkSession spark) throws IOException {
        FileSystem fs = factoryFileSystem();
        Path dealsPath = new Path("/deals.json");
        fs.copyFromLocalFile(
                new Path("C:\\Users\\David\\Music\\deals.json"),
                dealsPath
        );
        fs.close();
        spark.read()
                .json(HDFS + dealsPath)
                .write()
                .parquet(HDFS + "/deals-parquet.parquet");
        spark.close();
    }

    @SneakyThrows
    private static FileSystem factoryFileSystem() {
        Configuration configuration = new Configuration();
        configuration.setBoolean("dfs.client.use.datanode.hostname", true);
        return FileSystem.get(new URI(HDFS), configuration);
    }
}
