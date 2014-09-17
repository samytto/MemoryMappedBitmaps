/*
 * (c) Samy Chambi, Daniel Lemire, Owen Kaser.
 * 
 */
/**
 * Benchmarks on real data. 
 */
import it.uniroma3.mat.extendedset.intset.ConciseSet;
import it.uniroma3.mat.extendedset.intset.ImmutableConciseSet;
import it.uniroma3.mat.extendedset.intset.IntSet.IntIterator;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;

import net.sourceforge.sizeof.SizeOf;

import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.buffer.MutableRoaringBitmap;


public class Benchmark {

	private static final int nbRepetitions = 100;
	private static final long warmup_ms = 1000L;
	private static int careof=0;
	private static ImmutableRoaringBitmap[] irbs = null;
	private static ArrayList<ImmutableConciseSet> icss = null;
	private static ImmutableRoaringBitmap irb = null;
	private static ImmutableConciseSet ics = null;
	
	public static void main(String[] args) {
        boolean sizeOf = true;
        try {
                SizeOf.setMinSizeToLog(0);
                SizeOf.skipStaticField(false);
                SizeOf.skipFinalField(false); 
                SizeOf.skipFlyweightObject(false);
                SizeOf.deepSizeOf(args);
        } catch (IllegalStateException e) {
                sizeOf = false;
                System.out
                        .println("# disabling sizeOf, run  -javaagent:lib/SizeOf.jar or equiv. to enable");
        	}       

	    try {						
			String dataSources[] = {"census1881.csv","census-income.csv","weather_sept_85.csv"};
			
			System.out.println("\nResults interpretation :: \n"
            		+ "RAM Size = the required RAM space, in KB and bytes/bitmap, to store the 200 bitmaps\n"
            		+ "Disk Size = the required disk space, in MB and KB/bitmap, to store the 200 serialized bitmaps\n"
            		+ "Horizontal unions time = average time in ms to compute the horizontal union of 200 bitmaps\n"
            		+ "Intersections time = average time in ms to compute the intersection of 200 bitmaps\n"
            		+ "Scans time = average time in ms to scan the 200 bitmaps\n\n");
					
			final RealDataRetriever dataRetriever = new RealDataRetriever(args[0]);
			int[][] datum = new int[200][];//Once filled, the same data will be used for Roaring and Concise 
			for(int i=0; i<dataSources.length; i++) {
				final String dataSet = dataSources[i];
				//************ Roaring part ****************
			{
				File file = File.createTempFile("roarings", "bin");
				file.deleteOnExit();
				final FileOutputStream fos = new FileOutputStream(file);
				final DataOutputStream dos = new DataOutputStream(fos);
				ArrayList<Long> offsets = new ArrayList<Long>();	
				long serializationTime = 0;
				//Building 200 RoaringBitmaps 
				for (int j=0; j<200; j++) {					
					int[] data = dataRetriever.fetchBitPositions(dataSet, j);
					datum[j] = data;
					final MutableRoaringBitmap rb = MutableRoaringBitmap.bitmapOf(data);
					rb.trim();
					//Measuring the serialization's average time
					serializationTime += (long) test(new Launcher() {
						@Override
						public void launch() {
						try {
								serialzeRoaring(rb);
							} catch (IOException e) {e.printStackTrace();}
						}
					});
					offsets.add(fos.getChannel().position());
					rb.serialize(dos);
				}
				long lastOffset = fos.getChannel().position();
				dos.close();
				
				RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "r");
				MappedByteBuffer mbb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, lastOffset);			
				//RAM storage
                long sizeRAM = 0;
                irbs = new ImmutableRoaringBitmap[200];
                int i_rb = 0;
				for(int k=0; k < offsets.size(); k++) {
					mbb.position((int)offsets.get(k).longValue());
					final ByteBuffer bb = mbb.slice(); 
					long offsetLimit = k < 199 ? offsets.get(k+1) : lastOffset;
					bb.limit((int) (offsetLimit-offsets.get(k)));
					
					ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb);
					irbs[i_rb] = irb;
					i_rb++;
					if(sizeOf) sizeRAM += (SizeOf.deepSizeOf(irb));					
				}
				
				irbs = Arrays.copyOfRange(irbs, 0, i_rb);
				//Disk storage
				long sizeDisk = file.length();
				//Horizontal unions between 200 Roaring bitmaps
				long horizUnionTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						irb = BufferFastAggregation.horizontal_or(irbs);
						careof+=irb.getCardinality(); 
                    }
				});
				//Intersections between 200 Roaring bitmaps
				double intersectTime = test(new Launcher() {
					@Override
                    public void launch() {
						irb = BufferFastAggregation.and(irbs);
						careof+=irb.getCardinality(); 
                    }
				});	
				//Average time to retrieve set bits
				long scanTime = testScanRoaring();
				System.out.println("***************************");
				System.out.println("Roaring bitmap on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.printf("RAM Size = %4.2f KB (%4.2f bytes/bitmap)\n", (float)sizeRAM/1024.0, (float)sizeRAM/200.0);
				System.out.printf("Disk Size = %4.2f MB (%4.2f  KB/bitmap)\n", (float)sizeDisk/(1024.0*1024.0), ((float)sizeDisk/200.0)/1024.0);
				System.out.println("Serialization time = "+(serializationTime/200)+" ms/bitmap");
				System.out.println("Horizontal unions time = "+horizUnionTime+" ms");
				System.out.println("Intersections time = "+intersectTime+" ms");
				System.out.println("Scans time = "+scanTime+" ms");
				System.out.println(".ignore = "+careof);
				mbb = null;
				memoryMappedFile.close();
				file.delete();
			}
				//***************** ConciseSet part **********************************
			{	
				File file = File.createTempFile("conciseSets", "bin");
				file.deleteOnExit();
				final FileOutputStream fos = new FileOutputStream(file);
				final DataOutputStream dos = new DataOutputStream(fos);				
				ArrayList<Long> offsets = new ArrayList<Long>();
				long serializationTime = 0;
				//Building 200 ConciseSets 
				for (int j=0; j<200; j++) {					
					final ConciseSet cs = toConcise(datum[j]);
					offsets.add(fos.getChannel().position());
					final ImmutableConciseSet ics = ImmutableConciseSet.newImmutableFromMutable(cs);
					//Measuring average time to serialize into memory
					serializationTime += (long) test(new Launcher() {
						@Override
						public void launch() {
						try {
								serializeICS(ics);
							} catch (IOException e) {e.printStackTrace();}
						}
					});
					byte[] ICS = ics.toBytes();
					dos.write(ICS);
					dos.flush();
				}
				long lastOffset = fos.getChannel().position();
				dos.close();                
                //RAM storage
                long sizeRAM = 0;
                icss = new ArrayList<ImmutableConciseSet>();
                RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "r");
				MappedByteBuffer mbb = memoryMappedFile.getChannel().
										map(FileChannel.MapMode.READ_ONLY, 0, lastOffset);
				for(int k=0; k < offsets.size(); k++) {
					mbb.position((int)offsets.get(k).longValue());
					final ByteBuffer bb = mbb.slice();
					long offsetLimit = k < 199 ? offsets.get(k+1) : lastOffset;
					bb.limit((int) (offsetLimit-offsets.get(k)));
					ImmutableConciseSet ics = new ImmutableConciseSet(bb);
					icss.add(ics);
					if(sizeOf)	sizeRAM += (SizeOf.deepSizeOf(ics));
				}
				//Disk storage
				long sizeDisk = file.length();
				//Average time to compute unions between 200 ConciseSets
				long unionTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						ics = ImmutableConciseSet.union(icss.iterator());
						careof+=ics.size(); 
                    }
				});
				//Average time to compute intersects between 200 ConciseSets
				long intersectTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						ics = ImmutableConciseSet.intersection(icss.iterator());
						careof+=ics.size(); 
                    }
				});
				//Average time to retrieve set bits
				long scanTime = testScanConcise();
				
				System.out.println("***************************");
				System.out.println("ConciseSet on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.printf("RAM Size = %4.2f KB (%4.2f bytes/bitmap)\n", (float)sizeRAM/1024.0, (float)sizeRAM/200.0);
				System.out.printf("Disk Size = %4.2f MB (%4.2f  KB/bitmap)\n", (float)sizeDisk/(1024.0*1024.0), ((float)sizeDisk/200.0)/1024.0);
				System.out.println("Serialization time = "+(serializationTime/200)+" ms/bitmap");
				System.out.println("Unions time = "+unionTime+" ms");
				System.out.println("Intersections time = "+intersectTime+" ms");
				System.out.println("Scans time = "+scanTime+" ms");
				System.out.println(".ignore = "+careof);
				mbb = null;
                memoryMappedFile.close();
                file.delete();
		}
			}			
		} catch (IOException e) {e.printStackTrace();}		
	}
	
	static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat)
                ans.add(i);
        return ans;
	}
	
	@SuppressWarnings("resource")
	static void serialzeRoaring(MutableRoaringBitmap mrb) throws IOException{
		ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
		mrb.serialize(new DataOutputStream(new OutputStream(){
            ByteBuffer mBB;
            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
            public void close() {}
            public void flush() {}
            public void write(int b) {mBB.put((byte) b);}
            public void write(byte[] b) {}            
            public void write(byte[] b, int off, int l) {}
        }.init(outbb)));
	}
	
	static void serializeICS(ImmutableConciseSet ics) throws IOException{
		byte[] b = ics.toBytes();
		ByteBuffer outbb = ByteBuffer.allocate(b.length);
		@SuppressWarnings("resource")
		DataOutputStream dos = new DataOutputStream(new OutputStream() {
			ByteBuffer mBB;
            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}           
            public void write(int b) {mBB.put((byte) b);}
        }.init(outbb));
		
		dos.write(b);
	}
	
	static double test(Launcher job) {
        long jobTime, begin, end;
        int i, repeat=1;
        //Warming up the cache 
        do {
            repeat *= 2;
            begin = System.currentTimeMillis();
            for (int r = 0; r < repeat; r++) {
                    job.launch();
            }
            end = System.currentTimeMillis();
            jobTime = (end - begin)/nbRepetitions;
        } while (jobTime < warmup_ms);
        
        //We can start timings now 
        begin = System.currentTimeMillis();
        for (i = 0; i < nbRepetitions; ++i){
                job.launch();
        }
        end = System.currentTimeMillis();
        jobTime = end - begin;
        return (double)(jobTime) / (double)(nbRepetitions);
	}
	
	static long testScanRoaring() {
        long scanTime, begin, end;
        int i, k, repeat=1;
        org.roaringbitmap.IntIterator it;
        //Warming up the cache 
        do {
            repeat *= 2;
            scanTime = 0;
            for (int r = 0; r < repeat; r++) {
            	for(k=0; k<irbs.length; k++){
					irb = irbs[k];
					it = irb.getIntIterator();
					begin = System.currentTimeMillis();
					while(it.hasNext())
					{
						it.next();
					}
					end = System.currentTimeMillis();
					scanTime+=end-begin;
				}
            }
            scanTime/=nbRepetitions;
        } while (scanTime < warmup_ms);
        
        //We can start timings now 
        scanTime = 0;
		for(i=0; i<nbRepetitions; i++) {
			for(k=0; k<irbs.length; k++) {
				irb = irbs[k];
				it = irb.getIntIterator();
				begin = System.currentTimeMillis();
				while(it.hasNext())
				{
					it.next();
				}
				end = System.currentTimeMillis();
				scanTime+=end-begin;
			}
		}
		scanTime/=nbRepetitions;
        
        return scanTime;
	}

	static long testScanConcise() {
        long scanTime, begin, end;
        int i, k, repeat=1;
        IntIterator it;
        //Warming up the cache 
        do {
            repeat *= 2;
            scanTime = 0;
            for (int r = 0; r < repeat; r++) {
            	for(k=0; k<icss.size(); k++){
					ics = icss.get(k);
					it = ics.iterator();
					begin = System.currentTimeMillis();
					while(it.hasNext())
					{
						it.next();
					}
					end = System.currentTimeMillis();
					scanTime+=end-begin;
				}
            }
            scanTime/=nbRepetitions;
        } while (scanTime < warmup_ms);
        
        //We can start timings now 
        scanTime = 0;
		for(i=0; i<nbRepetitions; i++) {
			for(k=0; k<icss.size(); k++){
				ics = icss.get(k);
				it = ics.iterator();
				begin = System.currentTimeMillis();
				while(it.hasNext())
				{
					it.next();
				}
				end = System.currentTimeMillis();
				scanTime+=end-begin;
			}
		}
		scanTime/=nbRepetitions;
        
        return scanTime;
	}
	
	abstract static class Launcher {
		public abstract void launch();
	}
}