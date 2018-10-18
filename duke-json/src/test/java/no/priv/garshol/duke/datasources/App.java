package no.priv.garshol.duke.datasources;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.xml.sax.SAXException;

/**
 * Hello world!
 *
 */

import no.priv.garshol.duke.*;
import no.priv.garshol.duke.comparators.Levenshtein;
import no.priv.garshol.duke.datasources.InMemoryDataSource;
import no.priv.garshol.duke.matchers.PrintMatchListener;

public class App 
{
//    public static void main( String[] args )
//    {
//    	try {
//    	InMemoryDataSource source1 = new InMemoryDataSource() ;
//    	  InMemoryDataSource source2 = new InMemoryDataSource();
//    	 // TestUtils listener;
//    	  
//    	  
//    	String path = "C:\\Users\\SONY VAIO\\git\\Duke\\duke-json\\config-database.xml";
//        Configuration config;
//		
//			System.out.println("########### Load configuration #########");
//			config = ConfigLoader.load(path);
//			
//			
//			// TODO load new config  listener = new TestUtils.TestListener();
//		    Levenshtein comp = new Levenshtein();
//		    List<Property> props = new ArrayList();
//		    List<Property> props1 = new ArrayList();
//		    props.add(new PropertyImpl("ID"));
//		    props.add(new PropertyImpl("NAME", comp, 0.3, 0.8));
//		    props.add(new PropertyImpl("EMAIL", comp, 0.3, 0.8));
//
//		   // config = new ConfigurationImpl();
//		   props1 = config.getProperties();
//		   System.out.println("$$$$$$$$$$$"+props1.toString());
//		//    config.setThreshold(0.85);
//		 //   config.setMaybeThreshold(0.8);
//
//		    source1 = new InMemoryDataSource();
//		    source2 = new InMemoryDataSource();
//		   System.out.println("$$$$$$$$$$$$$$$$Data sources ##:"+ config.getDataSources().toString());
//		
//		   System.out.println("$$$$$$Data source $$$$$$:"+config.getDatabase(false));
//		   //    config.addDataSource(1, source1);
//		 //   config.addDataSource(2, source2);
//		    
//		    Processor proc = new Processor(config, true);
//		//    processor.addMatchListener(listener); //  List<TestUtils.Pair> matches = listener.getMatches();
//		    source2.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
//		    source2.add(TestUtils.makeRecord("ID", "3", "NAME", "xxxxx", "EMAIL", "bbbbb"));
//		    
//		  //stream method from APIs
//		  		String sentence = "Program aaaaa bbbbb";
//		  		Stream<String> wordStream1 = Pattern.compile("\\W").splitAsStream(sentence);
//		  		String[] wordArr = wordStream1.toArray(String[]::new);
//		  		System.out.println(Arrays.toString(wordArr));
//		  		System.out.println("Stream in words  :"+wordArr[0]+wordArr[1]+wordArr[2]);
//		  		
//		  	//	Record r = new 
//		    
//		    source1.add(TestUtils.makeRecord("ID", wordArr[0], "NAME",wordArr[1] , "EMAIL", wordArr[2]));
//		    
//		  //  source1.add(TestUtils.makeRecord("ID", "2", "NAME", "aaaaa", "EMAIL", "bbbbb"));
//		    
//		    System.out.println("RecordLinkTest.testContinuosStreams: source1:"+source1);
//		    System.out.println("RecordLinkTest.testContinuosStreams: source2:"+source2);
////		   
////		    matches = listener.getMatches();
////		    System.out.println("################Bad record count #########"+matches.size());
//	
//       
//        
//        System.out.println("########### Proc created  #########");
//        proc.addMatchListener(new PrintMatchListener(true, true, true, false,
//                                                     config.getProperties(),
//                                                     true));
//        System.out.println("########### Duplicate now  #########");
//        proc.deduplicate();
//        System.out.println("########### Dedupe completed #########");
//        proc.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (SAXException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//      }
}
