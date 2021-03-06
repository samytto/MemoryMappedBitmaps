/*
 * (c) Samy Chambi, Daniel Lemire.
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

	private static final int nbRepetitions = 1000;
	private static final long warmup_ms = 1000L;
	private static int careof=0;
	private static ImmutableRoaringBitmap[] irbs = null;
	private static ArrayList<ImmutableConciseSet> icss = null;
	private static ImmutableRoaringBitmap irb = null;
	private static ImmutableConciseSet ics = null;
	private static int repeatSerial = 100;
	private static int repeatDeser = 200;
	
	public static void main(String[] args) {
        boolean sizeOf = true;
        int NumberOfBitmaps = 200;
        try {
                SizeOf.setMinSizeToLog(0);
                SizeOf.skipStaticField(true);
                SizeOf.deepSizeOf(args);
        } catch (IllegalStateException e) {
                sizeOf = false;
                System.out
                        .println("# disabling sizeOf, run  -javaagent:lib/SizeOf.jar or equiv. to enable");

        }       

	    try {						
			String dataSources[] = {"census1881.csv","census-income.csv","weather_sept_85.csv"};
					
			RealDataRetriever dataRetriever = new RealDataRetriever(args[0]);
			int [][] datum = new int[NumberOfBitmaps][];
			for(int i=0; i<dataSources.length; i++) {
				String dataSet = dataSources[i];
				//************ Roaring part ****************
			{
				File file = File.createTempFile("roarings", "bin");
				file.deleteOnExit();
				final FileOutputStream fos = new FileOutputStream(file);
				final DataOutputStream dos = new DataOutputStream(fos);								
				ArrayList<Long> offsets = new ArrayList<Long>();
				long serializationTime = 0;
				//Building NumberOfBitmaps RoaringBitmaps 
				for (int j=0; j<NumberOfBitmaps; j++) {					
					int[] data = dataRetriever.fetchBitPositions(dataSet, j);
					datum[j] = data.clone();
					final MutableRoaringBitmap rb = MutableRoaringBitmap.bitmapOf(data);
					rb.trim();
					//Measuring the memory serialization's average time
					long time=0, cpt=0;
					while(cpt++<repeatSerial){
					try {
						long bef = System.nanoTime();
						ByteBuffer bb = serializeRoaring(rb);
						careof+=bb.capacity();
						long aft = System.nanoTime();
						time += aft-bef;
						} catch (IOException e) {e.printStackTrace();}
					}
					serializationTime+= time/repeatSerial;
					offsets.add(fos.getChannel().position());
					rb.serialize(dos);
					dos.flush();
				}
				long lastOffset = fos.getChannel().position();
				dos.close();
				RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "r");
				MappedByteBuffer mbb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, lastOffset);
                //RAM space used in bytes
                long sizeRAM = 0;
                irbs = new ImmutableRoaringBitmap[NumberOfBitmaps];
                int i_rb = 0;
                double deserializationTime = 0.0;
				for(int k=0; k < offsets.size(); k++) {
					mbb.position((int)offsets.get(k).longValue());
					final ByteBuffer bb = mbb.slice(); 
					long offsetLimit = k < 199 ? offsets.get(k+1) : lastOffset;
					bb.limit((int) (offsetLimit-offsets.get(k)));
					//Measuring deserializationTime
					long time=0; int cpt=0;
					while(cpt++<repeatDeser) {
						long bef = System.nanoTime();
						ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb.slice());
						careof+=irb.getCardinality();
						long aft = System.nanoTime();
						if(cpt>100) time += aft-bef;
					}
					deserializationTime+= (double)time/(double)repeatDeser;
					ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb);
					irbs[i_rb] = irb;
					i_rb++;
					if(sizeOf) sizeRAM += (SizeOf.deepSizeOf(irb));
				}				
				irbs = Arrays.copyOfRange(irbs, 0, i_rb);
				//Disk space used in bytes
				long sizeDisk = file.length();
				//Average time to compute the union of NumberOfBitmaps Roarings
				long horizUnionTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						irb = BufferFastAggregation.horizontal_or(irbs);
						careof+=irb.getCardinality(); 
                    }
				});
				//Average time to compute the intersection of NumberOfBitmaps Roarings
				double intersectTime = test(new Launcher() {
					@Override
                    public void launch() {
						irb = BufferFastAggregation.and(irbs);
						careof+=irb.getCardinality(); 
                    }
				});
	            //Average time to retrieve set bits
				double scanTime = testScanRoaring();
				System.out.println("***************************");
				System.out.println("Roaring bitmap on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.println("RAM Size = "+(sizeRAM/1024)+" Kbytes"+" ("+Math.round(sizeRAM*1./NumberOfBitmaps)+" bytes/bitmap)");
				System.out.println("Disk Size = "+(sizeDisk/1024)+" Kbytes"+" ("+Math.round(sizeDisk*1./NumberOfBitmaps)+" bytes/bitmap)");
				System.out.println("Serialization time = "+((serializationTime/NumberOfBitmaps)/1000)+" ms/bitmap");
				System.out.println("Deserialization time = "+((deserializationTime/NumberOfBitmaps)/1000)+" ms/bitmap");
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
				//Building NumberOfBitmaps ConciseSets 
				for (int j=0; j<NumberOfBitmaps; j++) {					
					ConciseSet cs = toConcise(datum[j]);					
					//Measuring the memory serialization's average time
					final ImmutableConciseSet ics = ImmutableConciseSet.newImmutableFromMutable(cs);
					long time=0, cpt=0;					
					while(cpt++<repeatSerial) {
					try {
						long bef = System.nanoTime();
						ByteBuffer bb = serializeICS(ics);
						careof+=bb.capacity();
						long aft = System.nanoTime();
						time += aft-bef;
						} catch (IOException e) {e.printStackTrace();}
					}
					serializationTime+= time/repeatSerial;
					offsets.add(fos.getChannel().position());
					byte[] ICS = ics.toBytes();
					dos.write(ICS);
					dos.flush();
				}
				long lastOffset = fos.getChannel().position();
				dos.close();               
                //RAM storage in bytes
                long sizeRAM = 0;
                icss = new ArrayList<ImmutableConciseSet>();
                RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "r");
				MappedByteBuffer mbb = memoryMappedFile.getChannel().
										map(FileChannel.MapMode.READ_ONLY, 0, lastOffset);
				double deserializationTime = 0.0;
				for(int k=0; k < offsets.size(); k++) {
					mbb.position((int)offsets.get(k).longValue());
					final ByteBuffer bb = mbb.slice();
					long offsetLimit = k < 199 ? offsets.get(k+1) : lastOffset;
					bb.limit((int) (offsetLimit-offsets.get(k)));
					//Measuring deserializationTime
					long time=0; int cpt=0;	                
					while(cpt++<repeatDeser) {
						long bef = System.nanoTime();
						ImmutableConciseSet ics = new ImmutableConciseSet(bb.slice());
						careof+=irb.getCardinality();
						long aft = System.nanoTime();
						if(cpt>100) time += aft-bef;
					}
					deserializationTime+= (double)time/(double)repeatDeser;
					ImmutableConciseSet ics = new ImmutableConciseSet(bb);
					icss.add(ics);
					if(sizeOf)	sizeRAM += (SizeOf.deepSizeOf(ics));
				}
				//Disk storage in bytes
				long sizeDisk = file.length();
				//Average time to compute the union of NumberOfBitmaps ConciseSets
				long unionTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						ics = ImmutableConciseSet.union(icss.iterator());
						careof+=ics.size(); 
                    }
				});
				//Average time to compute the intersection of NumberOfBitmaps ConciseSets
				long intersectTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						ics = ImmutableConciseSet.intersection(icss.iterator());
						careof+=ics.size(); 
                    }
				});
				//Average time to retrieve set bits
				double scanTime = testScanConcise();
				
				System.out.println("***************************");
				System.out.println("ConciseSet on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.println("RAM Size = "+(sizeRAM/1024)+" Kbytes"+" ("+Math.round(sizeRAM*1./NumberOfBitmaps)+" bytes/bitmap)");
				System.out.println("Disk Size = "+(sizeDisk/1024)+" Kbytes"+" ("+Math.round(sizeDisk*1./NumberOfBitmaps)+" bytes/bitmap)");
				System.out.println("Serialization time = "+((serializationTime/NumberOfBitmaps)/1000)+" ms/bitmap");
				System.out.println("Deserialization time = "+((deserializationTime/NumberOfBitmaps)/1000)+" ms/bitmap");
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
	
	static double test(Launcher job) {
        long jobTime, begin, end;
        int i, repeat=1;
        //Warming up the cache 
        do {
            repeat *= 2;// potentially unsafe for very large integers
            begin = System.currentTimeMillis();
            for (int r = 0; r < repeat; r++) {
                    job.launch();
            }
            end = System.currentTimeMillis();
            jobTime = (end - begin);
        } while ((jobTime < warmup_ms) && (repeat < (1<<24)));
        //We can start timings now 
        begin = System.currentTimeMillis();
        for (i = 0; i < nbRepetitions; ++i) {
                job.launch();
        }
        end = System.currentTimeMillis();
        jobTime = end - begin;
        return (double)(jobTime) / (double)(nbRepetitions);
	}
	
	static double testScanRoaring() {
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
        } while ((scanTime < warmup_ms) && (repeat<(1<<24)));
        
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
        return scanTime*1./nbRepetitions;
	}

	static double testScanConcise() {
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
        } while ((scanTime < warmup_ms)&&(repeat<(1<<24)));
        
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
        return scanTime*1./nbRepetitions;
	}

	@SuppressWarnings("resource")
	static ByteBuffer serializeRoaring(MutableRoaringBitmap mrb) throws IOException {
			ByteBuffer outbb = ByteBuffer.allocate(mrb.serializedSizeInBytes());
			DataOutputStream dos = new DataOutputStream(new OutputStream(){
	        ByteBuffer mBB;
	        OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}
	        	public void close() {}
	            public void flush() {}
	            public void write(int b) {mBB.put((byte) b);}
	            public void write(byte[] b) {}            
	            public void write(byte[] b, int off, int l) {}
	        }.init(outbb));
			mrb.serialize(dos);
			dos.close();
			
			return outbb;
		}
		
	static ByteBuffer serializeICS(ImmutableConciseSet ics) throws IOException {
			byte[] b = ics.toBytes();
			ByteBuffer outbb = ByteBuffer.allocate(b.length);
			@SuppressWarnings("resource")
			DataOutputStream dos = new DataOutputStream(new OutputStream() {
				ByteBuffer mBB;
	            OutputStream init(ByteBuffer mbb) {mBB=mbb; return this;}           
	            public void write(int b) {mBB.put((byte) b);}
	        }.init(outbb));
			
			dos.write(b);
			dos.close();
			
			return outbb;
		}
	
	abstract static class Launcher {
		public abstract void launch();
	}
}
