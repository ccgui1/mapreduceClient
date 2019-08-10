package com.ccgui.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FilterOutputFormat;

public class WordcountDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//1. ��ȡ������Ϣ�Լ���װ����
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		//2. ����jar����·��
		job.setJarByClass(WordcountDriver.class);
		
		//3. ����map��reduce��
		job.setMapperClass(WordcountMapper.class);
		job.setReducerClass(WordcountReducer.class);
	
		//4. ����map���
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		//5. ����reduce���
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);;
		//6. ������������·��
		FileInputFormat.setInputPaths(job, new Path(args [0]));
		FileOutputFormat.setOutputPath(job, new Path(args [1]));
		
		//7. �ύ
		boolean result = job.waitForCompletion(true);
		
		System.out.println(result ? 0 : 1);
	}
}