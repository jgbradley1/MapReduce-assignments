import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;
import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfStrings;

public class StripesPMI extends Configured implements Tool {
    private static final Logger LOG = Logger.getLogger(PairsPMI.class);

    // Mapper: emits (word, 1) for each unique word in a document (i.e. will not double count words in a doc)
    private static class MyMapper extends Mapper<LongWritable, Text, Text, HMapSIW> {

        // Reuse objects to save overhead of object creation.
        private static final FloatWritable ONE = new FloatWritable(1.0f);
        private static final PairOfStrings PAIR = new PairOfStrings();

        private static final Text KEY = new Text();
        private static final HMapSIW MAP = new HMapSIW();


        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = ((Text) value).toString();
            StringTokenizer itr = new StringTokenizer(line);

            // each mapper must have it's own copy of docWords since docWords only maintains information about 1 doc at a time
            List<String> docWords = new LinkedList<String>();

            // Clean out all duplicate words in the document
            while (itr.hasMoreTokens()) {
                String term = itr.nextToken();

                if (!docWords.contains(term)) {
                    docWords.add(term);
                }
            }

            // sort docWords alphabetically -- a requirement so that word pairs are not double counted
            Collections.sort(docWords);

            // Emit word counts and bigram counts
            for (int i = 0; i < docWords.size(); i++) {
                String term = docWords.get(i);

                // This will count P(x)
                MAP.put("*",  1);

                // This will count P(x, y)
                for (int j = i+1; j < docWords.size(); j++) {
                    MAP.put(docWords.get(j), 1);
                }

                KEY.set(term);
                context.write(KEY, MAP);

            }

            // Clean out entire hashmap
            docWords.clear();
            MAP.clear();
        }
    }

    // Reducer: sums up all the counts for each pair.
    // if pair is of the form (x, *), then will emit a total count of x in the vocabulary
    // if pair is of the form (x, y), then will emit the calculation P(x,y)/P(x)
    private static class MyReducer extends Reducer<Text, HMapSIW, PairOfStrings, FloatWritable> {

        // Reuse objects.
        private final static PairOfStrings KEY = new PairOfStrings();
        private final static FloatWritable VALUE = new FloatWritable(0.0f);
        private float marginal = 0.0f;

        @Override
        public void reduce(Text key, Iterable<HMapSIW> values, Context context)
                throws IOException, InterruptedException {

            // Sum up values.
            Iterator<HMapSIW> iter = values.iterator();
            HashMap<String, Integer> globalDocCount = new HashMap<String, Integer>();
            
            while (iter.hasNext()) {
                marginal += 1;
                
                HMapSIW hmap = iter.next();
                
                for (String word : hmap.keySet()) {
                    if (globalDocCount.containsKey(word)) {
                        globalDocCount.put(word, globalDocCount.get(word)+1);
                    }
                    else 
                        globalDocCount.put(word, 1);
                }
            }
            
            
            // emit final count of all (x, *) pairs
            KEY.set(key.toString(), "*");
            VALUE.set(marginal);
            context.write(KEY,  VALUE);

            for (String word : globalDocCount.keySet()) {

                // emit P(x,y)/P(x) for each (x, y) pair
                if (globalDocCount.get(word) >= 10) {
                    KEY.set(key.toString(), word);

                    float p_x = marginal/156215.0f;
                    float p_xy = globalDocCount.get(word)/156215.0f;
                    float temp = (float)globalDocCount.get(p_xy/p_x);

                    VALUE.set(temp);
                    context.write(KEY, VALUE);
                }
            }
        }
    }
    
    protected static class MyCombiner extends Reducer<Text, HMapSIW, Text, HMapSIW> {
        
        private static final HMapSIW newMap = new HMapSIW();
        
        @Override
        public void reduce(Text key, Iterable<HMapSIW> values, Context context)
                throws IOException, InterruptedException {
            
            for (HMapSIW map : values) {
                for (String word : map.keySet()) {
                    if (newMap.containsKey(word)) {
                        newMap.put(word, newMap.get(word) + map.get(word));
                    }
                    else
                        newMap.put(word,  map.get(word));
                }
            }
            context.write(key, newMap);
        }
    }

    protected static class MyPartitioner extends Partitioner<PairOfStrings, FloatWritable> {
        @Override
        public int getPartition(PairOfStrings key, FloatWritable value, int numReduceTasks) {
            return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }



    /**
     * Creates an instance of this tool.
     */
    public StripesPMI() {}

    private static final String INPUT = "input";
    private static final String OUTPUT = "output";
    private static final String NUM_REDUCERS = "numReducers";
    
    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INPUT));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(OUTPUT));
        options.addOption(OptionBuilder.withArgName("num").hasArg()
                .withDescription("number of reducers").create(NUM_REDUCERS));

        CommandLine cmdline;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            return -1;
        }

        if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(this.getClass().getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }

        String inputPath = cmdline.getOptionValue(INPUT);
        String outputPath = cmdline.getOptionValue(OUTPUT);
        int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ? Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;


        LOG.info("Tool: " + PairsPMI.class.getSimpleName());
        LOG.info(" - input path: " + inputPath);
        LOG.info(" - output path: " + outputPath);
        LOG.info(" - number of reducers: " + reduceTasks);

        //#################################################################################
        // Job 1 Configuration
        Configuration conf = getConf();
        Job job = Job.getInstance(conf);
        job.setJobName(PairsPMI.class.getSimpleName());
        job.setJarByClass(PairsPMI.class);

        job.setNumReduceTasks(reduceTasks);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(HMapSIW.class);
        job.setOutputKeyClass(PairOfStrings.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(MyMapper.class);
        //job.setCombinerClass(MyCombiner.class);
        //job.setPartitionerClass(MyPartitioner.class);
        job.setReducerClass(MyReducer.class);


        // Delete the output directories if they exists already.
        Path outputDir = new Path(outputPath);
        FileSystem.get(conf).delete(outputDir, true);


        long startTime = System.currentTimeMillis();
        if (job.waitForCompletion(true)) {
            LOG.info("Job #1 Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        }
        else {
            LOG.info("ERROR - Job #1 did not finish");
        }

        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new StripesPMI(), args);
    }
}