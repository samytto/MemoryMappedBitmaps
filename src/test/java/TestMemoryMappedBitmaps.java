import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet.IntIterator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

public class TestMemoryMappedBitmaps {

    static ImmutableRoaringBitmap[] getRoaring() {
        ImmutableRoaringBitmap[] answer = new ImmutableRoaringBitmap[roaringoffsets
                .size()];
        int i_rb = 0;
        for (int k = 0; k < roaringoffsets.size() - 1; k++) {
            roaringmbb.position((int) roaringoffsets.get(k).longValue());
            final ByteBuffer bb = roaringmbb.slice();
            bb.limit((int) (roaringoffsets.get(k + 1) - roaringoffsets.get(k)));
            ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb);
            answer[i_rb] = irb;
            i_rb++;
        }
        answer = Arrays.copyOfRange(answer, 0, i_rb);

        return answer;
    }

    static ArrayList<ImmutableConciseSet> getConcise() {
        ArrayList<ImmutableConciseSet> answer = new ArrayList<ImmutableConciseSet>();

        for (int k = 0; k < conciseoffsets.size() - 1; k++) {
            concisembb.position((int) conciseoffsets.get(k).longValue());
            final ByteBuffer bb = concisembb.slice();
            bb.limit((int) (conciseoffsets.get(k + 1) - conciseoffsets.get(k)));
            ImmutableConciseSet ics = new ImmutableConciseSet(bb);
            answer.add(ics);
        }

        return answer;
    }

    @Test
    public void basicTest() {
        System.out
                .println("Testing the serialization gives the same results...");
        ArrayList<ImmutableConciseSet> x = getConcise();
        ImmutableRoaringBitmap[] y = getRoaring();
        Assert.assertEquals(x.size(), y.length);
        for (int k = 0; k < y.length; ++k) {
            Assert.assertArrayEquals(y[k].toArray(), datum[k]);
            Assert.assertArrayEquals(y[k].toArray(), toArray(x.get(k)));
        }
    }

    @Test
    public void unionTest() {
        System.out.println("Testing that unions give the same results...");

        ArrayList<ImmutableConciseSet> x = getConcise();
        ImmutableRoaringBitmap[] y = getRoaring();

        for (int k = 1; k < y.length; k += 50) {
            ImmutableRoaringBitmap[] yk = Arrays.copyOf(y, k);
            List<ImmutableConciseSet> xk = x.subList(0, k);
            ImmutableConciseSet ax = ImmutableConciseSet.union(xk.iterator());
            ImmutableRoaringBitmap ay = BufferFastAggregation.horizontal_or(yk);
            Assert.assertArrayEquals(ay.toArray(), toArray(ax));
        }
    }

    @Test
    public void intersectionTest() {
        System.out
                .println("Testing that intersections give the same results...");

        ArrayList<ImmutableConciseSet> x = getConcise();
        ImmutableRoaringBitmap[] y = getRoaring();

        for (int k = 1; k < y.length; k += 50) {
            ImmutableRoaringBitmap[] yk = Arrays.copyOf(y, k);
            List<ImmutableConciseSet> xk = x.subList(0, k);
            ImmutableConciseSet ax = ImmutableConciseSet.intersection(xk
                    .iterator());
            ImmutableRoaringBitmap ay = BufferFastAggregation.and(yk);
            Assert.assertArrayEquals(ay.toArray(), toArray(ax));
        }
    }

    @AfterClass
    public static void clearFiles() {
        System.out.println("Cleaning memory-mapped file.");
        datum = null;
        try {
            roaringmemoryMappedFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        roaringfile.delete();
        roaringoffsets = null;
        roaringmbb = null;
        try {
            concisememoryMappedFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        concisefile.delete();
        conciseoffsets = null;
        concisembb = null;
    }

    @BeforeClass
    public static void initFiles() throws IOException {
        System.out
                .println("Setting up memory-mapped file. (Can take some time.)");
        String dataSources = "census1881.csv";// we verify with just one
                                              // data set

        RealDataRetriever dataRetriever = new RealDataRetriever(location);
        datum = new int[200][];

        String dataSet = dataSources;
        System.out.println("Recovering the data...");

        for (int j = 0; j < 200; j++) {
            int[] data = dataRetriever.fetchBitPositions(dataSet, j);
            datum[j] = data.clone();
        }
        System.out.println("Building the roaring bitmaps...");
        long aft, bef;
        bef = System.currentTimeMillis();

        // ************ Roaring part ****************
        {
            roaringfile = File.createTempFile("roarings", "bin");
            roaringfile.deleteOnExit();
            final FileOutputStream fos = new FileOutputStream(roaringfile);
            final DataOutputStream dos = new DataOutputStream(fos);
            roaringoffsets = new ArrayList<Long>();
            // Building 200 RoaringBitmaps
            for (int j = 0; j < 200; j++) {
                RoaringBitmap rb = RoaringBitmap.bitmapOf(datum[j]);
                rb.trim();
                if (!Arrays.equals(rb.toArray(), datum[j]))
                    throw new RuntimeException("bug");
                roaringoffsets.add(fos.getChannel().position());
                rb.serialize(dos);
                dos.flush();
            }
            long lastOffset = fos.getChannel().position();
            dos.close();
            roaringmemoryMappedFile = new RandomAccessFile(roaringfile, "r");
            roaringmbb = roaringmemoryMappedFile.getChannel().map(
                    FileChannel.MapMode.READ_ONLY, 0, lastOffset);
        }
        aft = System.currentTimeMillis();
        System.out.println("It took " + (aft - bef) + " ms");
        bef = System.currentTimeMillis();

        {
            System.out.println("Building the concise bitmaps...");
            concisefile = File.createTempFile("conciseSets", "bin");
            concisefile.deleteOnExit();
            final FileOutputStream fos = new FileOutputStream(concisefile);
            final DataOutputStream dos = new DataOutputStream(fos);
            conciseoffsets = new ArrayList<Long>();
            // Building 200 ConciseSets
            for (int j = 0; j < 200; j++) {
                ConciseSet cs = toConcise(datum[j]);
                if (!Arrays.equals(toArray(cs), datum[j]))
                    throw new RuntimeException("bug");
                conciseoffsets.add(fos.getChannel().position());
                int[] ints = cs.getWords();
                for (int k = 0; k < ints.length; k++)
                    dos.writeInt(ints[k]);
                dos.flush();
            }
            long lastOffset = fos.getChannel().position();
            dos.close();
            concisememoryMappedFile = new RandomAccessFile(concisefile, "r");
            concisembb = concisememoryMappedFile.getChannel().map(
                    FileChannel.MapMode.READ_ONLY, 0, lastOffset);

        }
        aft = System.currentTimeMillis();
        System.out.println("It took " + (aft - bef) + " ms");

    }

    static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat)
            ans.add(i);
        return ans;
    }

    static int[] toArray(ImmutableConciseSet s) {
        ArrayList<Integer> answer = new ArrayList<Integer>();
        IntIterator i = s.iterator();
        while (i.hasNext())
            answer.add(i.next());
        int[] ta = new int[answer.size()];
        for (int k = 0; k < ta.length; ++k)
            ta[k] = answer.get(k);
        return ta;
    }

    static int[] toArray(ConciseSet s) {
        ArrayList<Integer> answer = new ArrayList<Integer>();
        IntIterator i = s.iterator();
        while (i.hasNext())
            answer.add(i.next());
        int[] ta = new int[answer.size()];
        for (int k = 0; k < ta.length; ++k)
            ta[k] = answer.get(k);
        return ta;
    }

    static int[][] datum;

    public static final String location = "real-roaring-datasets";
    static File concisefile;
    static ArrayList<Long> conciseoffsets;
    static RandomAccessFile concisememoryMappedFile;
    static MappedByteBuffer concisembb;

    static File roaringfile;
    static ArrayList<Long> roaringoffsets;
    static RandomAccessFile roaringmemoryMappedFile;
    static MappedByteBuffer roaringmbb;
}
