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
import java.util.Iterator;

import net.sourceforge.sizeof.SizeOf;

import org.roaringbitmap.buffer.BufferFastAggregation;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;


public class Benchmark {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {						
			String dataSources[] = {"census1881.csv","census-income.csv","uscensus2000.csv","weather_sept_85.csv","wikileaks-noquotes.csv"};
			int nbRepetitions = 20;			
			RealDataRetriever dataRetriever = new RealDataRetriever(args[0]);
			int [][] datum = new int[200][];
			for(int i=0; i<dataSources.length; i++) {
				int careof = 0;
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
					datum[j] = data;
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
                //ArrayList<ImmutableRoaringBitmap> irbs = new ArrayList<ImmutableRoaringBitmap>();
                ImmutableRoaringBitmap[] irbs = new ImmutableRoaringBitmap[200];
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
				long sizeDisque = file.length();
				//Unions between 200 Roaring bitmaps
				long unionTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {
				//ImmutableRoaringBitmap irb = irbs.get(0);
				long bef = System.currentTimeMillis();
				BufferFastAggregation.or(irbs);
				/*for (int j=1; j<irbs.size()-1; j++) {
					irb = ImmutableRoaringBitmap.or(irb, irbs.get(j));
					careof+=irb.getCardinality();
				}*/
				long aft = System.currentTimeMillis();
				unionTime+=aft-bef;
				}
				unionTime/=nbRepetitions;
				//Intersections between 200 Roaring bitmaps
				long intersectTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {
				//ImmutableRoaringBitmap irb = irbs.get(0);				
				long bef = System.currentTimeMillis();
				BufferFastAggregation.and(irbs);
				/*for (int j=1; j<irbs.size()-1; j++) {
					irb = ImmutableRoaringBitmap.and(irb, irbs.get(j));
					careof+=irb.getCardinality();
				}*/
				long aft = System.currentTimeMillis();
				intersectTime+=aft-bef;
				}
				intersectTime/=nbRepetitions;
				//Average time to retrieve set bits
				long scanTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {
				for(int k=0; k<irbs.length; k++){
					ImmutableRoaringBitmap irb = irbs[k];//irbs.get(k);
					Iterator<Integer> it = irb.iterator();
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
				System.out.println("Roaring bitmap on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.println("RAM Size = "+sizeRAM+" bytes");
				System.out.println("Disque Size = "+sizeDisque+" bytes");
				System.out.println("Unions time = "+unionTime+" ms");
				System.out.println("Intersctions time = "+intersectTime+" ms");
				System.out.println("Scans time = "+scanTime+" ms");
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
				long sizeDisque = file.length();
				//Average time to compute unions between 200 Roaring bitmaps
				long unionTime = 0;
				for(int rep=0; rep<nbRepetitions; rep++) {				
				long bef = System.currentTimeMillis();
				ImmutableConciseSet ics = ImmutableConciseSet.union(icss.iterator());							
				long aft = System.currentTimeMillis();
				unionTime+=aft-bef;
				careof+=ics.size();	
				}
				unionTime/=nbRepetitions;
				//Average time to compute intersects between 200 Roaring bitmaps
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
				System.out.println("CociseSet on "+dataSet+" dataset");
				System.out.println("***************************");
				System.out.println("RAM Size = "+sizeRAM+" bytes");
				System.out.println("Disque Size = "+sizeDisque+" bytes");
				System.out.println("Unions time = "+unionTime+" ms");
				System.out.println("Intersctions time = "+intersectTime+" ms");
				System.out.println("Scans time = "+scanTime+" ms");
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

}
