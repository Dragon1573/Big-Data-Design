package io.github.dragon1573;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * HBase数据库导入程序
 *
 * @author Dragon1573
 */
@SuppressWarnings("deprecation")
public class HBaseImporter extends Thread {
    private HTable table;

    private HBaseImporter() {
        Configuration configuration = HBaseConfiguration.create();
        try {
            table = new HTable(configuration, Bytes.toBytes("national"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new Exception("You must set input path!");
        }
        String fileName = args[args.length - 1];
        HBaseImporter importer = new HBaseImporter();
        importer.importLocalFileToHbase(fileName);
    }

    private void importLocalFileToHbase(final String fileName) {
        long st = System.currentTimeMillis();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(
                new InputStreamReader(new FileInputStream(fileName))
            );
            String line;
            int count = 0;
            while ((line = reader.readLine()) != null) {
                count++;
                put(line);
                if (count % 10000 == 0) {
                    System.out.println(count);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } try {
                table.flushCommits();
                table.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + "ms. ");
    }

    private void put(final String line) throws IOException {
        String[] arr = line.split(",", -1);
        String[] column = {"id", "name", "year", "gender", "count"};
        if (arr.length == column.length) {
            Put put = new Put(Bytes.toBytes(arr[0]));
            for (int i = 1; i < arr.length; i++) {
                put.add(
                    Bytes.toBytes("col_group"),
                    Bytes.toBytes(column[i]),
                    Bytes.toBytes(arr[i])
                );
            } table.put(put);
        }
    }
}
