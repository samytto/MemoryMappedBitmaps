
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import net.sourceforge.sizeof.SizeOf;

import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.RoaringBitmap;

/**
 * O. Kaser's benchmark over real data
 * 
 */
public class BenchmarkBrouillon {
        static final String AND = "AND";
        static final String OR = "OR";
        static final String XOR = "XOR";
        static final String serialize = "serialize";
        static final String deserialize = "deserialize";
        static final String[] ops = {serialize, /*deserialize,*/AND, OR, XOR };
        static final String CONCISE = "CONCISE";
        static final String ROARING = "ROARING";
        static final String[] formats = {  CONCISE, ROARING };

        static int junk = 0; // to fight optimizer.

        static long LONG_ENOUGH_NS = 1000L * 1000L * 1000L;

        @SuppressWarnings("javadoc")
        public static void main(final String[] args) {

                Locale.setDefault(Locale.US);

                System.out.println("########");
                System.out.println("# " + System.getProperty("java.vendor")
                        + " " + System.getProperty("java.version") + " "
                        + System.getProperty("java.vm.name"));
                System.out.println("# " + System.getProperty("os.name") + " "
                        + System.getProperty("os.arch") + " "
                        + System.getProperty("os.version"));
                System.out.println("# processors: "
                        + Runtime.getRuntime().availableProcessors());
                System.out.println("# max mem.: "
                        + Runtime.getRuntime().maxMemory());
                System.out.println("########");

                String dataset = args[1];
                int NTRIALS = Integer.parseInt(args[2]);
                System.out.println(NTRIALS + " tests on " + dataset);
                // for future use...

                boolean sizeof = true;
                try {
                        SizeOf.setMinSizeToLog(0);
                        SizeOf.skipStaticField(true);
                        // SizeOf.skipFinalField(true);
                        SizeOf.deepSizeOf(args);
                } catch (IllegalStateException e) {
                        sizeof = false;
                        System.out
                                .println("# disabling sizeOf, run  -javaagent:lib/SizeOf.jar or equiv. to enable");

                }
                RealDataRetriever dataSrc = new RealDataRetriever(args[0]);
                HashMap<String, Double> totalTimes = new HashMap<String, Double>();
                HashMap<String, Double> totalSizes = new HashMap<String, Double>();
                for (String op : ops)
                        for (String format : formats) {
                                totalTimes.put(op + ";" + format, 0.0);
                                totalSizes.put(format, 0.0); // done more than
                                                             // necessary
                        }
                for (int i = 0; i < NTRIALS; ++i)
                        for (String op : ops)
                                for (String format : formats)
                                        test(op, format, totalTimes,
                                                totalSizes, sizeof,
                                                dataSrc.fetchBitPositions(
                                                        dataset, 0),
                                                dataSrc.fetchBitPositions(
                                                        dataset, 1));
                if (sizeof) {
                        System.out.println("Size ratios");
                        double baselineSize = totalSizes.get(ROARING);
                        for (String format : formats) {
                                double thisSize = totalSizes.get(format);
                                System.out.printf("%s\t%5.2f\n", format,
                                        thisSize / baselineSize);
                        }
                        System.out.println();
                }
                
                System.out.println("Time ratios");

                for (String op : ops) {
                        double baseline = totalTimes.get(op + ";" + ROARING);

                        System.out.println("baseline is " + baseline);
                        System.out.println(op);
                        System.out.println();
                        for (String format : formats) {
                                double ttime = totalTimes
                                        .get(op + ";" + format);
                                if (ttime != 0.0)
                                        System.out.printf("%s\t%s\t%5.2f\n",
                                                format, op, ttime / baseline);
                        }
                }
                System.out.println("ignore this " + junk);
        }

        static BitSet toBitSet(int[] dat) {
                BitSet ans = new BitSet();
                for (int i : dat)
                        ans.set(i);
                return ans;
        }

        static ConciseSet toConcise(int[] dat) {
                ConciseSet ans = new ConciseSet();
                for (int i : dat)
                        ans.add(i);
                return ans;
        }

        static ConciseSet toConciseWAH(int[] dat) {
                ConciseSet ans = new ConciseSet(true);
                for (int i : dat)
                        ans.add(i);
                return ans;
        }
        
        static ImmutableRoaringBitmap getImmutableRoaring(RoaringBitmap rb) {
        	//Building an immutableRoaringBitmap
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
        	DataOutputStream dos = new DataOutputStream(bos);
        	try {
				rb.serialize(dos);
				dos.close();
        	} catch (IOException e3) {e3.printStackTrace();}
        	ByteBuffer bb = ByteBuffer.wrap(bos.toByteArray());
        	ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb);
        	return irb;
        }

        static ImmutableConciseSet getImmutableConcise(ConciseSet cs) {
        	//Building an immutableConciseSet bitmap
        	ImmutableConciseSet ics = ImmutableConciseSet.newImmutableFromMutable(cs);
        	return ics;
        }
        
        /*
         * What follows is ugly and repetitive, but it has the virtue of being
         * straightforward.
         */

        static void test(String op, String format,
                Map<String, Double> totalTimes, Map<String, Double> totalSizes,
                boolean sizeof, int[] data1, int[] data2) {
                String timeKey = op + ";" + format;
                String spaceKey = format;


                /***************************************************************************/
                if (format.equals(ROARING)) {
                        final RoaringBitmap bm1 = RoaringBitmap.bitmapOf(data1);
                        final RoaringBitmap bm2 = RoaringBitmap.bitmapOf(data2);
                        bm1.trim();
                        bm2.trim();                        
                        final ImmutableRoaringBitmap irb1 = getImmutableRoaring(bm1);
                    	final ImmutableRoaringBitmap irb2 = getImmutableRoaring(bm2);
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0; 
                        if(op.equals(serialize)) {
                        	thisTime = avgSeconds(new Computation() {
                                @Override
                                public void compute() {
                                	ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                	DataOutputStream dos = new DataOutputStream(bos);
                                	try {
                        				bm1.serialize(dos);
                        				dos.close();
                                	} catch (IOException e3) {e3.printStackTrace();}                          	                                        
                                }
                        });
                        totalTimes.put(timeKey,
                                thisTime + totalTimes.get(timeKey));
                        }
                        else if (op.equals(AND)) {                        	
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                RoaringBitmap result = ImmutableRoaringBitmap
                                                        .and(irb1, irb2);
                                                BenchmarkBrouillon.junk += result
                                                        .getCardinality(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                RoaringBitmap result = ImmutableRoaringBitmap
                                                        .or(irb1, irb2);
                                                BenchmarkBrouillon.junk += result
                                                        .getCardinality(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                RoaringBitmap result = ImmutableRoaringBitmap
                                                        .xor(irb1, irb2);
                                                BenchmarkBrouillon.junk += result
                                                        .getCardinality(); // cheap
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        } else
                                throw new RuntimeException("unknown op " + op);
                }
                /***************************************************************************/
                else if (format.equals(CONCISE)) {
                        final ConciseSet bm1 = toConcise(data1);
                        final ConciseSet bm2 = toConcise(data2);
                        final ImmutableConciseSet ics1 = getImmutableConcise(bm1);
                        final ImmutableConciseSet ics2 = getImmutableConcise(bm2);
                        if (sizeof) {
                                long theseSizesInBits = 8 * (SizeOf
                                        .deepSizeOf(bm1) + SizeOf
                                        .deepSizeOf(bm2));
                                totalSizes.put(spaceKey, theseSizesInBits
                                        + totalSizes.get(spaceKey));
                        }
                        double thisTime = 0.0;
                        if(op.equals(serialize)) {
                        	thisTime = avgSeconds(new Computation() {
                                @Override
                                public void compute() {
                                	ByteArrayOutputStream bos = new ByteArrayOutputStream();
                                	DataOutputStream dos = new DataOutputStream(bos);
                                	try {
                        				bm1.serialize(dos);
                        				dos.close();
                                	} catch (IOException e3) {e3.printStackTrace();}                          	                                        
                                }
                        });
                        totalTimes.put(timeKey,
                                thisTime + totalTimes.get(timeKey));
                        }
                        else if (op.equals(AND)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ImmutableConciseSet result = ImmutableConciseSet.intersection(ics1, ics2);
                                                BenchmarkBrouillon.junk += result.size(); // cheap???
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                        if (op.equals(OR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ImmutableConciseSet result = ImmutableConciseSet.union(ics1, ics2);

                                                BenchmarkBrouillon.junk += result.size(); // dunno
                                                                            // if
                                                                            // cheap
                                                                            // enough
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }
                        /*if (op.equals(XOR)) {
                                thisTime = avgSeconds(new Computation() {
                                        @Override
                                        public void compute() {
                                                ConciseSet result = ImmutableConciseSet.symmetricDifference(ics1, ics2);

                                                Benchmark.junk += result
                                                        .isEmpty() ? 1 : 0; // cheap???
                                        }
                                });
                                totalTimes.put(timeKey,
                                        thisTime + totalTimes.get(timeKey));
                        }*/
                }
        }

        static double avgSeconds(Computation toDo) {
                int ntrials = 1;
                long elapsedNS = 0L;
                long start, stop;
                do {
                        ntrials *= 2;
                        start = System.nanoTime();
                        for (int i = 0; i < ntrials; ++i) {
                                // might have to do something to stop hoisting
                                // here
                                toDo.compute();
                        }
                        stop = System.nanoTime();
                        elapsedNS = stop - start;
                } while (elapsedNS < LONG_ENOUGH_NS);
                /* now things are very hot, so do an actual timing */
                start = System.nanoTime();
                for (int i = 0; i < ntrials; ++i) {
                        // danger, optimizer??
                        toDo.compute();
                }
                stop = System.nanoTime();
                return (stop - start) / (ntrials * 1e+9); // ns to s
        }

        abstract static class Computation {
                public abstract void compute(); // must mess with "junk"
        }

}