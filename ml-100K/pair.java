import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.net.MalformedURLException;
import java.net.URL;

public class wordCountEliminateHTMLURL {

	static boolean checkURL(String word)
	{
		int urlFlag = 0; //Initialized to not an URL
		try {
             		URL url = new URL(word);
			urlFlag = 1;
         	} catch (MalformedURLException e) {
        	 	urlFlag = 0;
         	}
		if(word.startsWith("www") && word.indexOf(".") < word.lastIndexOf("."))
        		urlFlag = 1;
		if(urlFlag == 0)
        		return false;
        	return true;
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			String lineModified = "";  //New removed html tag line
		        String replaceVal = "";    //Used for Tag matching
	        	String wordCheck = "";
			int flag = 0; //indicator variable
			for(int k=0; k<line.length(); k++)
			{
				char c = line.charAt(k);
				if(c == '<')
				{
					flag = 1; //Activate flag
					replaceVal = "";
				}
				lineModified += c;
				replaceVal += c;
				if(c == '>' && flag == 1)
				{
					flag = 0;
					lineModified = lineModified.subSequence(0, lineModified.lastIndexOf(replaceVal)).toString();
				        replaceVal = "";
			        }
				if(c == ' ') // URL checking 
				{
			        	if(checkURL(wordCheck))
			    	 		lineModified = lineModified.subSequence(0, lineModified.lastIndexOf(wordCheck)).toString();	           	 
					wordCheck = "";
				}
				else
				{
		    			wordCheck += c;
				}
		 	}
			if(checkURL(wordCheck))
		    		lineModified = lineModified.subSequence(0, lineModified.lastIndexOf(wordCheck)).toString();
			
			line = lineModified;

			// Remove Punctuation 
			StringTokenizer tokenizer = new StringTokenizer(line, " []{}()\t\n\r\f|\",.:;?!@#$%^&*'<>!~`_-+/\\=");
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
				}
			}
		}
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				}
			output.collect(key, new IntWritable(sum));
			}
		}
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(wordCountEliminateHTMLURL.class);
		conf.setJobName("WordCountDriver");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("input"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		JobClient.runJob(conf);
		}
	}


