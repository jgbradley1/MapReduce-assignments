import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.util.ToolRunner;

import cern.colt.Arrays;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfWritables;

public class BooleanRetrievalCompressed {
    private final MapFile.Reader index;
    private final FSDataInputStream collection;
    private final Stack<Set<Integer>> stack;

    public BooleanRetrievalCompressed(String indexPath, String collectionPath, FileSystem fs) throws IOException {
        index = new MapFile.Reader(new Path(indexPath + "/part-r-00000"), fs.getConf());
        collection = fs.open(new Path(collectionPath));
        stack = new Stack<Set<Integer>>();
    }

    public void runQuery(String q) throws IOException {
        String[] terms = q.split("\\s+");

        for (String t : terms) {
            if (t.equals("AND")) {
                performAND();
            } else if (t.equals("OR")) {
                performOR();
            } else {
                pushTerm(t);
            }
        }

        Set<Integer> set = stack.pop();

        for (Integer i : set) {
            String line = fetchLine(i);
            System.out.println(i + "\t" + line);
        }
    }

    public void pushTerm(String term) throws IOException {
        stack.push(fetchDocumentSet(term));
    }

    public void performAND() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            if (s2.contains(n)) {
                sn.add(n);
            }
        }

        stack.push(sn);
    }

    public void performOR() {
        Set<Integer> s1 = stack.pop();
        Set<Integer> s2 = stack.pop();

        Set<Integer> sn = new TreeSet<Integer>();

        for (int n : s1) {
            sn.add(n);
        }

        for (int n : s2) {
            sn.add(n);
        }

        stack.push(sn);
    }

    public Set<Integer> fetchDocumentSet(String term) throws IOException {
        Set<Integer> set = new TreeSet<Integer>();

        for (PairOfVInts pair : fetchPostings(term)) {
            set.add(pair.getLeftElement());
        }

        return set;
    }

    public ArrayListWritable<PairOfVInts> fetchPostings(String term) throws IOException {
        Text key = new Text();
        ArrayListWritable<PairOfVInts> value = new ArrayListWritable<PairOfVInts>();

        key.set(term);
        index.get(key, value);

        return value;
    }

    public String fetchLine(long offset) throws IOException {
        collection.seek(offset);
        BufferedReader reader = new BufferedReader(new InputStreamReader(collection));

        return reader.readLine();
    }

    private static final String INDEX = "index";
    private static final String COLLECTION = "collection";

    @SuppressWarnings({ "static-access" })
    public static void main(String[] args) throws IOException {
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

        FileSystem fs = FileSystem.get(new Configuration());

        BooleanRetrievalCompressed s = new BooleanRetrievalCompressed(indexPath, collectionPath, fs);

        String[] queries = { "outrageous fortune AND", "white rose AND", "means deceit AND",
                "white red OR rose AND pluck AND", "unhappy outrageous OR good your AND OR fortune AND" };

        for (String q : queries) {
            System.out.println("Query: " + q);

            s.runQuery(q);
            System.out.println("");
        }
    }
}
