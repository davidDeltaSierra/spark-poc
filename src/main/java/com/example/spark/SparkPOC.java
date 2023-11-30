package com.example.spark;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.net.URI;

import static org.apache.spark.sql.functions.*;

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
        //fileSystemCopyFromLocalFile();
        //writeProductsAndNFESParquet(spark);
        //joinTables(spark);
        spark.close();
    }

    private static Dataset<Row> joinTables(SparkSession spark) {
        Dataset<Row> nfes = spark.read()
                .parquet(HDFS + "/nfes_parquet");
        Dataset<Row> products = spark.read()
                .parquet(HDFS + "/products_parquet")
                .withColumnRenamed("title", "p_title")
                .withColumnRenamed("total_amount", "p_total_amount");

        return nfes.join(products, col("qrcode").equalTo(col("nfe_qrcode")))
                .groupBy("qrcode", nfes.columns())
                .agg(collect_list(
                        struct(
                                col("p_title").alias("title"),
                                col("price"),
                                col("p_total_amount").alias("total_amount"),
                                col("discount_amount"),
                                col("tax_amount"),
                                col("quantity"),
                                col("unit"),
                                col("fields")
                        )
                ).alias("products"));
    }

    private static void writeProductsAndNFESParquet(SparkSession spark) {
        final long version = System.currentTimeMillis();
        final String partitionKey = "version";

        Dataset<Row> allJsonFilesDataset = spark.read()
                .option("multiline", true)
                .format("json")
                .load(HDFS + "/raw_notes_json/");

        allJsonFilesDataset.drop("products")
                .withColumn(partitionKey, lit(version))
                .write()
                .partitionBy(partitionKey)
                .mode(SaveMode.Append)
                .parquet(HDFS + "/nfes_parquet");

        allJsonFilesDataset.select(col("qrcode"), explode(col("products")).alias("product"))
                .select(col("qrcode").alias("nfe_qrcode"), col("product.*"))
                .withColumn(partitionKey, lit(version))
                .write()
                .partitionBy(partitionKey)
                .mode(SaveMode.Append)
                .parquet(HDFS + "/products_parquet");
    }

    @SneakyThrows
    private static void fileSystemCopyFromLocalFile() {
        FileSystem fs = factoryFileSystem();
        fs.copyFromLocalFile(
                new Path("C:\\Users\\David\\Music\\nfes\\ac.json"),
                new Path("/ac.json")
        );
        fs.copyFromLocalFile(
                new Path("C:\\Users\\David\\Music\\nfes\\nota.json"),
                new Path("/nota.json")
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
