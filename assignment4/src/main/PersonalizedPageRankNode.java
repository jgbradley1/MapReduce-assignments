import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import edu.umd.cloud9.io.array.ArrayListOfFloatsWritable;
import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.map.HMapIFW;

/**
 * Representation of a graph node for Personalized PageRank. 
 *
 * @author Joshua Bradley
 */
public class PersonalizedPageRankNode implements Writable {
    public static enum Type {
        Complete((byte) 0),  // PageRank mass and adjacency list.
        Mass((byte) 1),      // PageRank mass only.
        Structure((byte) 2); // Adjacency list only.

        public byte val;

        private Type(byte v) {
            this.val = v;
        }
    };

    private static final Type[] mapping = new Type[] { Type.Complete, Type.Mass, Type.Structure };

    private Type type;
    private int nodeid;
    private ArrayListOfIntsWritable adjacenyList;
    private HMapIFW sourceList;

    public PersonalizedPageRankNode() {
        sourceList = new HMapIFW();
    }

    public HMapIFW getsourceList() {
        return sourceList;
    }

    public void setsourceList(HMapIFW list) {
        this.sourceList = list;
    }

    public float getPageRank(int nodeID) {
        return sourceList.get(nodeID);
    }

    public void setPageRank(int nodeID, float value) {
        if (this.sourceList.containsKey(nodeID)) {
            this.sourceList.put(nodeID,  value);
        }
    }

    public int getNodeId() {
        return nodeid;
    }

    public void setNodeId(int n) {
        this.nodeid = n;
    }

    public ArrayListOfIntsWritable getAdjacenyList() {
        return adjacenyList;
    }

    public void setAdjacencyList(ArrayListOfIntsWritable list) {
        this.adjacenyList = list;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    /**
     * Deserializes this object.
     *
     * @param in source for raw byte representation
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int b = in.readByte();
        type = mapping[b];
        nodeid = in.readInt();
        sourceList = new HMapIFW();

        if (type.equals(Type.Mass)) {
            sourceList.readFields(in);
            return;
        }

        if (type.equals(Type.Complete)) {
            sourceList.readFields(in);
        }

        adjacenyList = new ArrayListOfIntsWritable();
        adjacenyList.readFields(in);
    }

    /**
     * Serializes this object.
     *
     * @param out where to write the raw byte representation
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeByte(type.val);
        out.writeInt(nodeid);

        if (type.equals(Type.Mass)) {
            sourceList.write(out);
            return;
        }

        if (type.equals(Type.Complete)) {
            sourceList.write(out);
        }

        adjacenyList.write(out);
    }

    @Override
    public String toString() {
        return String.format("{%d %s %s}",
                nodeid,
                (sourceList == null ? "[]" : sourceList.toString()),
                (adjacenyList == null ? "[]" : adjacenyList.toString(this.adjacenyList.size())));
    }


    /**
     * Returns the serialized representation of this object as a byte array.
     *
     * @return byte array representing the serialized representation of this object
     * @throws IOException
     */
    public byte[] serialize() throws IOException {
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        DataOutputStream dataOut = new DataOutputStream(bytesOut);
        write(dataOut);

        return bytesOut.toByteArray();
    }

    /**
     * Creates object from a <code>DataInput</code>.
     *
     * @param in source for reading the serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static PersonalizedPageRankNode create(DataInput in) throws IOException {
        PersonalizedPageRankNode m = new PersonalizedPageRankNode();
        m.readFields(in);

        return m;
    }

    /**
     * Creates object from a byte array.
     *
     * @param bytes raw serialized representation
     * @return newly-created object
     * @throws IOException
     */
    public static PersonalizedPageRankNode create(byte[] bytes) throws IOException {
        return create(new DataInputStream(new ByteArrayInputStream(bytes)));
    }
}
