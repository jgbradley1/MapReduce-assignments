import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Int2IntFrequencyDistributionEntry;

public class LookupPostingsCompressed extends Configured implements Tool {
    private static final String INDEX = "index";
    private static final String COLLECTION = "collection";

    private LookupPostingsCompressed() {}

    private static ArrayListWritable<PairOfInts> deserializePosting(BytesWritable inputBytes) {
        ArrayListWritable<PairOfInts> posting = new ArrayListWritable<PairOfInts>();

        DataInputStream dataIn = new DataInputStream(new ByteArrayInputStream(inputBytes.getBytes()));

        try {
            while (true) {
                int left = WritableUtils.readVInt(dataIn);
                int right = WritableUtils.readVInt(dataIn);

                if (right != 0) 
                    posting.add(new PairOfInts(left, right));
            }
        }
        catch (EOFException e){}
        catch (IOException e) {}

        try {
            dataIn.close();
        } catch (IOException e) {}

        return posting;
    }


    /**
     * Runs this tool.
     */
    @SuppressWarnings({ "static-access" })
    public int run(String[] args) throws Exception {
        Options options = new Options();

        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("input path").create(INDEX));
        options.addOption(OptionBuilder.withArgName("path").hasArg()
                .withDescription("output path").create(COLLECTION));

        CommandLine cmdline = null;
        CommandLineParser parser = new GnuParser();

        try {
            cmdline = parser.parse(options, args);
        } catch (ParseException exp) {
            System.err.println("Error parsing command line: " + exp.getMessage());
            System.exit(-1);
        }

        if (!cmdline.hasOption(INDEX) || !cmdline.hasOption(COLLECTION)) {
            System.out.println("args: " + Arrays.toString(args));
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(120);
            formatter.printHelp(LookupPostingsCompressed.class.getName(), options);
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(-1);
        }

        String indexPath = cmdline.getOptionValue(INDEX);
        String collectionPath = cmdline.getOptionValue(COLLECTION);

        if (collectionPath.endsWith(".gz")) {
            System.out.println("gzipped collection is not seekable: use compressed version!");
            System.exit(-1);
        }
        
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        MapFile.Reader reader = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), config);

        FSDataInputStream collection = fs.open(new Path(collectionPath));
        BufferedReader d = new BufferedReader(new InputStreamReader(collection));

        Text key = new Text();
        ArrayListWritable<PairOfInts> postings;
        BytesWritable bytesValue = new BytesWritable();
        
        System.out.println("Looking up postings for the term \"starcross'd\"");
        key.set("starcross'd");
        
        reader.get(key, bytesValue);
        postings = deserializePosting(bytesValue);
        
        
        //ArrayListWritable<PairOfVInts> postings = value;
        for (PairOfInts pair : postings) {
            System.out.println(pair);
            collection.seek(pair.getLeftElement());
            System.out.println(d.readLine());
        }
        
        bytesValue = new BytesWritable();
        key.set("gold");
        reader.get(key, bytesValue);
        postings = deserializePosting(bytesValue);
        System.out.println("Complete postings list for 'gold': (" + postings.size() + ", " + postings + ")");

        Int2IntFrequencyDistribution goldHist = new Int2IntFrequencyDistributionEntry();
        //postings = value;
        for (PairOfInts pair : postings) {
            goldHist.increment(pair.getRightElement());
        }
        
        System.out.println("histogram of tf values for gold");
        for (PairOfInts pair : goldHist) {
            System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
        }
        
        bytesValue = new BytesWritable();
        key.set("silver");
        reader.get(key, bytesValue);
        postings = deserializePosting(bytesValue);
        System.out.println("Complete postings list for 'silver': (" + postings.size() + ", " + postings + ")");

        Int2IntFrequencyDistribution silverHist = new Int2IntFrequencyDistributionEntry();
        //postings = value;
        for (PairOfInts pair : postings) {
            silverHist.increment(pair.getRightElement());
        }
        
        System.out.println("histogram of tf values for silver");
        for (PairOfInts pair : silverHist) {
            System.out.println(pair.getLeftElement() + "\t" + pair.getRightElement());
        }
        
        bytesValue = new BytesWritable();
        key.set("bronze");
        Writable w = reader.get(key,  bytesValue);

        if (w == null) {
            System.out.println("the term bronze does not appear in the collection");
        }
        
        collection.close();
        reader.close();
        
        return 0;
    }

    /**
     * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
     */
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new LookupPostingsCompressed(), args);
    }
}
