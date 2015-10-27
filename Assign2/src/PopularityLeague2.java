import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PopularityLeague2 extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new PopularityLeague2(),
				args);
		System.exit(res);
	}
	public static List<String> readHDFSLeague(String path, Configuration conf)
			throws IOException {
		Path pt = new Path(path);
		FileSystem fs = FileSystem.get(pt.toUri(), conf);
		FSDataInputStream file = fs.open(pt);
		BufferedReader buffIn = new BufferedReader(new InputStreamReader(file));

		List<String> everything = new ArrayList<String>();
		String line;
		while ((line = buffIn.readLine()) != null) {
			StringTokenizer tokens = new StringTokenizer(line, " ");
			while(tokens.hasMoreTokens())
			{
				everything.add(tokens.nextToken().trim());
			}
			
		}
		return everything;
	}
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();
		FileSystem fs = FileSystem.get(conf);
		Path outputPath = new Path(args[1]);
		fs.delete(outputPath, true);

		Job jobA = Job.getInstance(conf, "Link Count");
		jobA.setOutputKeyClass(IntWritable.class);
		jobA.setOutputValueClass(IntWritable.class);

		jobA.setMapperClass(LinkCountMap.class);
		jobA.setReducerClass(LinkCountReduce.class);
		jobA.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(jobA, new Path(args[0]));
		FileOutputFormat.setOutputPath(jobA, outputPath);

		jobA.setJarByClass(PopularityLeague2.class);
		return jobA.waitForCompletion(true) ? 0 : 1;
		//
		// Job jobB = Job.getInstance(conf, "Popularity League");
		// jobB.setOutputKeyClass(IntWritable.class);
		// jobB.setOutputValueClass(IntWritable.class);
		//
		// jobB.setMapOutputKeyClass(NullWritable.class);
		// jobB.setMapOutputValueClass(IntWritable.class);
		//
		// jobB.setMapperClass(PopularityLeagueMap.class);
		// jobB.setReducerClass(PopularityLeagueReduce.class);
		// jobB.setNumReduceTasks(1);
		//
		// FileInputFormat.setInputPaths(jobB, tmpPath);
		// FileOutputFormat.setOutputPath(jobB, new Path(args[2]));
		//
		// jobB.setInputFormatClass(KeyValueTextInputFormat.class);
		// jobB.setOutputFormatClass(TextOutputFormat.class);
		//
		// jobB.setJarByClass(PopularityLeague.class);
		// return jobB.waitForCompletion(true) ? 0 : 1;
	}

	public static class LinkCountMap extends
			Mapper<Object, Text, IntWritable, IntWritable> {
		List<String> league = new ArrayList<String>();

		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();

			String leaguePath = conf.get("league");

			this.league = readHDFSLeague(leaguePath, conf);
					
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			String input = value.toString();
			String[] pages = input.trim().split(":");
			int parentPage = Integer.parseInt(pages[0]);

			if (league.contains(parentPage)) {
				context.write(new IntWritable(Integer.valueOf(parentPage)),
						new IntWritable(0));
			}
			StringTokenizer tokenizer = new StringTokenizer(pages[1].trim(),
					" ");
			while (tokenizer.hasMoreTokens()) {
				if (league.contains(parentPage)) {
					context.write(
							new IntWritable(Integer.parseInt(tokenizer
									.nextToken().trim())), new IntWritable(1));
				}
			}
		}
	}

	public static class LinkCountReduce extends
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

		private List<Pair<Integer, Integer>> countToTitleList = new ArrayList<Pair<Integer, Integer>>();

		@Override
		public void reduce(IntWritable key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int popularity = 0;
			for (IntWritable pagesPointing : values) {
				popularity += pagesPointing.get();
			}
			if (popularity != 0) {
				countToTitleList.add(new Pair<Integer, Integer>(key.get(),
						popularity));
			}
			// context.write(key, new IntWritable(sum));
		}

		@Override
		protected void cleanup(
				Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			Collections.sort(countToTitleList,
					new Comparator<Pair<Integer, Integer>>() {
						public int compare(Pair<Integer, Integer> o1,
								Pair<Integer, Integer> o2) {
							int valueCompare = o1.second.compareTo(o2.second);
							// if (valueCompare == 0) {
							// valueCompare = o1.first.compareTo(o2.first);
							// }
							return valueCompare;
						}
					});

			for (int i = 0; i < countToTitleList.size(); i++) {
				Pair<Integer, Integer> pair = countToTitleList.get(i);
				context.write(new IntWritable(pair.first), new IntWritable(i));
			}
		}
	}

	
}

class Pair<A extends Comparable<? super A>, B extends Comparable<? super B>>
		implements Comparable<Pair<A, B>> {

	public final A first;
	public final B second;

	public Pair(A first, B second) {
		this.first = first;
		this.second = second;
	}

	public static <A extends Comparable<? super A>, B extends Comparable<? super B>> Pair<A, B> of(
			A first, B second) {
		return new Pair<A, B>(first, second);
	}

	@Override
	public int compareTo(Pair<A, B> o) {
		int cmp = o == null ? 1 : (this.first).compareTo(o.first);
		return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
	}

	@Override
	public int hashCode() {
		return 31 * hashcode(first) + hashcode(second);
	}

	private static int hashcode(Object o) {
		return o == null ? 0 : o.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof Pair))
			return false;
		if (this == obj)
			return true;
		return equal(first, ((Pair<?, ?>) obj).first)
				&& equal(second, ((Pair<?, ?>) obj).second);
	}

	private boolean equal(Object o1, Object o2) {
		return o1 == o2 || (o1 != null && o1.equals(o2));
	}

	@Override
	public String toString() {
		return "(" + first + ", " + second + ')';
	}
}
