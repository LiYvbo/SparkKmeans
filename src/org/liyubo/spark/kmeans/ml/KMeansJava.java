package org.liyubo.spark.kmeans.ml;

/**
 * Created by liyubo on 2016/7/12.
 */
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.SparkConf;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;

public class KMeansJava {
    public static void main(String[] args) throws Exception{
        SparkConf conf = new SparkConf().setAppName("K-means implement by Java").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Vector> type_0 = new ArrayList<Vector>();
        List<Vector> type_1 = new ArrayList<Vector>();
        List<Vector> type_2 = new ArrayList<Vector>();

        /*
        1. 加载数据
         */
        String path = "C:\\TempFolder\\testdata\\data\\sensor.txt";
        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<Vector> parsedData = data.map(
                new Function<String, Vector>() {
                    public Vector call(String s) {
                        String[] sarray = s.split(" ");
                        double[] values = new double[sarray.length];
                        for (int i = 0; i < sarray.length; i++)
                            values[i] = Double.parseDouble(sarray[i]);
                        return Vectors.dense(values);
                    }
                }
        );
        parsedData.cache();


        /*
        2. 执行聚类，聚成3类，并行30次
         */
        int numClusters = 3;
        int numIterations = 30;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        /*
        3. 计算错误率，评估聚类效果
         */
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        /*
        4. 聚类结果写入文件
        */
        //分类
        for(Vector v : parsedData.collect()){
            switch (clusters.predict(v)){
                case 0:type_0.add(v);break;
                case 1:type_1.add(v);break;
                case 2:type_2.add(v);break;
                default:break;
            }
        }

        //写文件
        System.out.println("++++++++++++++++++聚类结果写文件++++++++++++++++++");
        System.out.println("+------------------------------------------------");
        //聚类0结果处理
        File file0=new File("C:\\TempFolder\\testdata\\result-md\\dateset-0.txt");
        if(!file0.exists())
            file0.createNewFile();
        FileOutputStream out0=new FileOutputStream(file0,false); //如果追加方式用true
        StringBuffer sb0=new StringBuffer();
        sb0.append("================聚类0结果=================\n");
        for (Vector v:type_0){
            sb0.append(v + "\n");
        }

        out0.write(sb0.toString().getBytes("utf-8"));//注意需要转换对应的字符集
        out0.close();

        //聚类1结果处理
        File file1=new File("C:\\TempFolder\\testdata\\result-md\\dateset-1.txt");
        if(!file1.exists())
            file1.createNewFile();
        FileOutputStream out1=new FileOutputStream(file1,false); //如果追加方式用true
        StringBuffer sb1=new StringBuffer();
        sb1.append("================聚类1结果=================\n");
        for (Vector v:type_1){
            sb1.append(v+"\n");
        }

        out1.write(sb1.toString().getBytes("utf-8"));//注意需要转换对应的字符集
        out1.close();

        //聚类2结果处理
        File file2=new File("C:\\TempFolder\\testdata\\result-md\\dateset-2.txt");
        if(!file2.exists())
            file2.createNewFile();
        FileOutputStream out2=new FileOutputStream(file2,false); //如果追加方式用true
        StringBuffer sb2=new StringBuffer();
        sb2.append("================聚类2结果=================\n");
        for (Vector v:type_0){
            sb2.append(v+"\n");
        }

        out2.write(sb2.toString().getBytes("utf-8"));//注意需要转换对应的字符集
        out2.close();

        //关闭上下文
        sc.close();
    }
}