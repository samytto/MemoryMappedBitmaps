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


public class Copy_2_of_Benchmark {

	private static final int nbRepetitions = 100;
	private static int careof=0;
	private static ImmutableRoaringBitmap[] irbs = null;
	private static ArrayList<ImmutableConciseSet> icss = null;
	private static ImmutableRoaringBitmap irb = null;
	private static ImmutableConciseSet ics = null;
	
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
						irb = BufferFastAggregation.or(irbs);
						careof+=irb.getCardinality(); 
                    }
				});
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
				double scanTime = 0.0;
				for(int k=0; k<irbs.length; k++){
					irb = irbs[k];
					final org.roaringbitmap.IntIterator it = irb.getIntIterator();
					scanTime += test(new Launcher() {
						@Override
						public void launch() {
							while(it.hasNext())
							 {
								it.next();
							 } 
						}
					});
				}
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
                icss = new ArrayList<ImmutableConciseSet>();
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
				double scanTime = 0.0;
				for(int k=0; k<icss.size(); k++) {
					ics = icss.get(k);
					final IntIterator it = ics.iterator();
					scanTime += test(new Launcher() {
						@Override
						public void launch() {
							while(it.hasNext())
							 {
								it.next();
							 } 
						}
					});
				}
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
	
	static ConciseSet toConcise(int[] dat) {
        ConciseSet ans = new ConciseSet();
        for (int i : dat)
                ans.add(i);
        return ans;
	}
	
	static double test(Launcher job) {
        long jobTime, begin, end;
        final long warmup_iterations = 10;//100L * 1000L;
        //Warming up the cache 
        for(int j=0; j<warmup_iterations; j++) {
                begin = System.currentTimeMillis();
                for (int i = 0; i < nbRepetitions; ++i) {
                        job.launch();
                }
                end = System.currentTimeMillis();
                jobTime = end - begin;
        }
        //We can start timings now 
        begin = System.currentTimeMillis();
        for (int i = 0; i < nbRepetitions; ++i) {
                job.launch();
        }
        end = System.currentTimeMillis();
        jobTime = end - begin;
        return (double)(jobTime) / (double)(nbRepetitions);
	}
	
	abstract static class Launcher {
		public abstract void launch();
	}
}
