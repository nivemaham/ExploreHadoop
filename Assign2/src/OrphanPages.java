import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//import TitleCount.TitleCountMap;
//import TitleCount.TitleCountReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

// >>> Don't Change
public class OrphanPages extends Configured implements Tool {

	
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new OrphanPages(), args);
        System.exit(res);
    }
// <<< Don't Change

    @Override
    public int run(String[] args) throws Exception {
    	Job job = Job.getInstance(this.getConf(), "Orphan Pages");
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(LinkCountMap.class);
        job.setReducerClass(OrphanPageReduce.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(OrphanPages.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
    	@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String line = value.toString();
        	String[] pages = line.trim().split(":");
        	int parentPage = Integer.parseInt(pages[0]);
        	
        	context.write(new IntWritable(Integer.valueOf(parentPage)), new IntWritable(0));
            StringTokenizer tokenizer = new StringTokenizer(pages[1].trim(), " ");
            while(tokenizer.hasMoreTokens())
            {
            	context.write(new IntWritable(Integer.parseInt(tokenizer.nextToken().trim())), new IntWritable(1));
            }
        }
    }

    public static class OrphanPageReduce extends Reducer<IntWritable, IntWritable, IntWritable, NullWritable> {
       
    	@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int sum=0;
        	boolean isFirst =true;
        	int firstPage =0;
    		for(IntWritable pagesPointing : values)
        	{	
    			if(isFirst) firstPage= pagesPointing.get();
    			isFirst = false;
        		sum+=pagesPointing.get();
        	}
        	
        	if(sum==0)
        	{
        		context.write(key, NullWritable.get());
        	}
//        	else if(sum==1 && Integer.valueOf(firstPage).equals(Integer.valueOf(key.get())))
//        	{
//        		context.write(key, NullWritable.get());
//        	}
        }
    }
}