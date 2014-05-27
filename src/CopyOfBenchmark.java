/*
 * (c) Samy Chambi, Daniel Lemire.
 * 
 */

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

import net.sourceforge.sizeof.SizeOf;

import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.RoaringBitmap;


public class CopyOfBenchmark {

	private static final int nbRepetitions = 100;
	private static int careof=0;
	private static ImmutableRoaringBitmap[] irbs = null;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {						
			String dataSources[] = {"census1881.csv","census-income.csv","weather_sept_85.csv"};
					
			RealDataRetriever dataRetriever = new RealDataRetriever(args[0]);
			int [][] datum = new int[200][];
			for(int i=0; i<dataSources.length; i++) {
				String dataSet = dataSources[i];
				//************ Roaring part ****************
			{
				File file = File.createTempFile("roarings", "bin");
				file.deleteOnExit();
				final FileOutputStream fos = new FileOutputStream(file);
				final DataOutputStream dos = new DataOutputStream(fos);								
				ArrayList<Long> offsets = new ArrayList<Long>();				
				//Building 200 RoaringBitmaps 
				for (int j=0; j<200; j++) {					
					int[] data = dataRetriever.fetchBitPositions(dataSet, j);
					datum[j] = data.clone();
					RoaringBitmap rb = RoaringBitmap.bitmapOf(data);
					rb.trim();
					offsets.add(fos.getChannel().position());
					rb.serialize(dos);
					dos.flush();
				}
				long lastOffset = fos.getChannel().position();
				dos.close();
				RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "r");
				MappedByteBuffer mbb = memoryMappedFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, lastOffset);
			
				boolean sizeOf = true;
                try {
                        SizeOf.setMinSizeToLog(0);
                        SizeOf.skipStaticField(true);
                        SizeOf.deepSizeOf(args);
                } catch (IllegalStateException e) {
                        sizeOf = false;
                        System.out
                                .println("# disabling sizeOf, run  -javaagent:lib/SizeOf.jar or equiv. to enable");

                }		
              //RAM space used in bytes
                long sizeRAM = 0;
                irbs = new ImmutableRoaringBitmap[200];
                int i_rb = 0;
				for(int k=0; k < offsets.size()-1; k++) {
					mbb.position((int)offsets.get(k).longValue());
					final ByteBuffer bb = mbb.slice(); 
					bb.limit((int) (offsets.get(k+1)-offsets.get(k)));
					ImmutableRoaringBitmap irb = new ImmutableRoaringBitmap(bb);
					irbs[i_rb] = irb;
					i_rb++;
					sizeRAM += (SizeOf.deepSizeOf(irb));
				}
				irbs = Arrays.copyOfRange(irbs, 0, i_rb);
				//Disk space used in bytes
				long sizeDisk = file.length();
				//Unions between 200 Roaring bitmaps
				long unionTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						ImmutableRoaringBitmap irb = BufferFastAggregation.or(irbs);
						careof+=irb.getCardinality(); 
                    }
				});
				/*long unionTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {
					ImmutableRoaringBitmap irb;
					long bef = System.currentTimeMillis();
					irb = BufferFastAggregation.or(irbs);
					long aft = System.currentTimeMillis();
					careof+=irb.getCardinality();
					unionTime+=aft-bef;
				}
				unionTime/=nbRepetitions;*/
				//Horizontal unions between 200 Roaring bitmaps
				long horizUnionTime = (long) test(new Launcher() {
					@Override
                    public void launch() {
						ImmutableRoaringBitmap irb = BufferFastAggregation.horizontal_or(irbs);
						careof+=irb.getCardinality(); 
                    }
				});
				/*long horizUnionTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {
					ImmutableRoaringBitmap irb;
					long bef = System.currentTimeMillis();
					irb = BufferFastAggregation.horizontal_or(irbs);
					long aft = System.currentTimeMillis();
					careof+=irb.getCardinality();
					horizUnionTime+=aft-bef;
				}
				horizUnionTime/=nbRepetitions;*/
				//Intersections between 200 Roaring bitmaps
				double intersectTime = test(new Launcher() {
					@Override
                    public void launch() {
						ImmutableRoaringBitmap irb = BufferFastAggregation.and(irbs);
						careof+=irb.getCardinality(); 
                    }
				});
				/*double intersectTime = 0.0;
				for(int rep=0; rep<nbRepetitions; rep++) {
					ImmutableRoaringBitmap irb;				
					double bef = System.currentTimeMillis();
					irb = BufferFastAggregation.and(irbs);				
					double aft = System.currentTimeMillis();
					careof+=irb.getCardinality();
					intersectTime+=aft-bef;
				}
				intersectTime/=nbRepetitions;*/
				//Average time to retrieve set bits
				long scanTime = 0;
				for(int k=0; k<irbs.length; k++){
					ImmutableRoaringBitmap irb = irbs[k];
					final org.roaringbitmap.IntIterator it = irb.getIntIterator();
					scanTime += (long) test(new Launcher() {
						@Override
						public void launch() {
							while(it.hasNext())
							 {
								it.next();
							 } 
						}
					});
				}
				/*long scanTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {
				for(int k=0; k<irbs.length; k++){
					ImmutableRoaringBitmap irb = irbs[k];//irbs.get(k);
					org.roaringbitmap.IntIterator it = irb.getIntIterator();
					long bef = System.currentTimeMillis();
					while(it.hasNext())
					 {
						it.next();
					 }
					long aft = System.currentTimeMillis();
					scanTime+=aft-bef;
				}
				}
				scanTime/=nbRepetitions;*/
				System.out.println("***************************");
				System.out.println("Roaring bitmap on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.println("RAM Size = "+(sizeRAM/1024)+" Kbytes"+" ("+(sizeRAM/200)+" bytes/bitmap)");
				System.out.println("Disk Size = "+(sizeDisk/1024)+" Kbytes"+" ("+(sizeDisk/200)+" bytes/bitmap)");
				System.out.println("Unions time = "+unionTime+" ms");
				System.out.println("Horizontal unions time = "+horizUnionTime+" ms");
				System.out.println("Intersections time = "+intersectTime+" ms");
				System.out.println("Scans time = "+scanTime+" ms");
				System.out.println(".ignore = "+careof);
			}
				//***************** ConciseSet part **********************************
			{	
				File file = File.createTempFile("conciseSets", "bin");
				file.deleteOnExit();
				final FileOutputStream fos = new FileOutputStream(file);
				final DataOutputStream dos = new DataOutputStream(fos);				
				ArrayList<Long> offsets = new ArrayList<Long>();
				//Building 200 ConciseSets 
				for (int j=0; j<200; j++) {					
					ConciseSet cs = toConcise(datum[j]);
					offsets.add(fos.getChannel().position());
					int[] ints = cs.getWords();
					for(int k=0; k<ints.length; k++)
						dos.writeInt(ints[k]);
					dos.flush();
				}
				long lastOffset = fos.getChannel().position();
				dos.close();                
                //RAM storage in bytes
                long sizeRAM = 0;
                ArrayList<ImmutableConciseSet> icss = new ArrayList<ImmutableConciseSet>();
                RandomAccessFile memoryMappedFile = new RandomAccessFile(file, "r");
				MappedByteBuffer mbb = memoryMappedFile.getChannel().
										map(FileChannel.MapMode.READ_ONLY, 0, lastOffset);
				for(int k=0; k < offsets.size()-1; k++) {
					mbb.position((int)offsets.get(k).longValue());
					final ByteBuffer bb = mbb.slice();
					bb.limit((int) (offsets.get(k+1)-offsets.get(k)));
					ImmutableConciseSet ics = new ImmutableConciseSet(bb);
					icss.add(ics);
					sizeRAM += (SizeOf.deepSizeOf(ics));
				}
				//Disk storage in bytes
				long sizeDisk = file.length();
				//Average time to compute unions between 200 ConciseSets
				long unionTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {				
					long bef = System.currentTimeMillis();
					ImmutableConciseSet ics = ImmutableConciseSet.union(icss.iterator());							
					long aft = System.currentTimeMillis();
					unionTime+=aft-bef;
					careof+=ics.size();	
				}
				unionTime/=nbRepetitions;
				//Average time to compute intersects between 200 ConciseSets
				long intersectTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {				
					long bef = System.currentTimeMillis();
					ImmutableConciseSet ics = ImmutableConciseSet.intersection(icss.iterator());
					long aft = System.currentTimeMillis();
					careof+=ics.size();
					intersectTime+=aft-bef;
				}
				intersectTime/=nbRepetitions;
				
				//Average time to retrieve set bits
				long scanTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {
				for(int k=0; k<icss.size(); k++){
					ImmutableConciseSet ics = icss.get(k);					
					IntIterator it = ics.iterator();
					long bef = System.currentTimeMillis();
					while(it.hasNext())
					 {
						it.next();
					 }
					long aft = System.currentTimeMillis();
					scanTime+=aft-bef;
				}
				}
				scanTime/=nbRepetitions;
				System.out.println("***************************");
				System.out.println("ConciseSet on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.println("RAM Size = "+(sizeRAM/1024)+" Kbytes"+" ("+(sizeRAM/200)+" bytes/bitmap)");
				System.out.println("Disk Size = "+(sizeDisk/1024)+" Kbytes"+" ("+(sizeDisk/200)+" bytes/bitmap)");
				System.out.println("Unions time = "+unionTime+" ms");
				System.out.println("Intersections time = "+intersectTime+" ms");
				System.out.println("Scans time = "+scanTime+" ms");
				System.out.println(".ignore = "+careof);
		}
			}			
		} catch (IOException e) {e.printStackTrace();}
		
	}
	
	static void SerializeConciseSet(ConciseSet cs, DataOutputStream dos) {	
		int[] ints = cs.getWords();
		for(int k=0; k<ints.length; k++)
			try {
				for(int j=0; j<4; j++)
					dos.writeInt(ints[k]);
				dos.flush();
			} catch (IOException e) {e.printStackTrace();}		
	}
	
	static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat)
                ans.add(i);
        return ans;
	}
	
	static double test(Launcher job) {
		long jobTime = 0;
        long begin, end;
        final long warmup_iterations = 100L * 1000L;
        //Warming up the cache 
        for(int j=0; j<warmup_iterations; j++) {
                begin = System.currentTimeMillis();
                for (int i = 0; i < nbRepetitions; ++i) {
                        job.launch();
                }
                end = System.currentTimeMillis();
                jobTime = begin - end;
        }
        //We can start timings now 
        begin = System.currentTimeMillis();
        for (int i = 0; i < nbRepetitions; ++i) {
                job.launch();
        }
        end = System.currentTimeMillis();
        jobTime = begin - end;
        return (jobTime) / (nbRepetitions);
	}
	
	abstract static class Launcher {
		public abstract void launch();
	}
}
