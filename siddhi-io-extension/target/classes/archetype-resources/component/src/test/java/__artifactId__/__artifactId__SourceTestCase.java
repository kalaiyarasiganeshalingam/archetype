package ${package}.${artifactId};

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
//import org.wso2.siddhi.core.util.SiddhiTestHelper;

/*import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.LineNumberReader;*/

import java.util.concurrent.atomic.AtomicInteger;

public class ${artifactId}SourceTestCase {
    private static final Logger log = Logger.getLogger(${siddhi_function_name}SourceTestCase.class);
    private AtomicInteger count = new AtomicInteger();
    private AtomicInteger count1 = new AtomicInteger();
    private boolean paused;

    @Test
    public void testXmlInputDefaultMapping() throws InterruptedException {
        log.info("______________Test to read the files and their contents under the given folder______________");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', folder.path='Files', file.extension='xml', @map(type='xml')) " +
                "define stream FooStream (symbol string,  price float,  volume int); " +
                "define stream BarStream (symbol string,  price float,  volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream",  new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    int n = count1.getAndIncrement();
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        //assert event count
        AssertJUnit.assertEquals("Number of events", 3, count1.get());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testJsonInputDefaultMapping() throws InterruptedException {
        log.info("______________Test to read the files and their contents under the given folder______________");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', folder.path='Files', file.extension='json', @map(type='json')) " +
                "define stream FooStream (symbol string,  price float,  volume int); " +
                "define stream BarStream (symbol string,  price float,  volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    int n = count.getAndIncrement();
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        //assert event count
        AssertJUnit.assertEquals("Number of events", 4 , count.get());
        siddhiAppRuntime.shutdown();
    }

    /*@Test
    public void testXmlInputDefaultMappingwithPaused() throws InterruptedException {
        log.info("______________Test to read the files and their contents under the given folder using paused" +
                "______________");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', folder.path='Files', file.extension='xml', @map(type='xml')) " +
                "define stream FooStream (symbol string,  price float,  volume int); " +
                "define stream BarStream (symbol string,  price float,  volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    int n = count.getAndIncrement();
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("ATM", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        try {
            File f = new File("Files/text.xml");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            LineNumberReader  lnr = new LineNumberReader(new FileReader(f));
            for (int i  = 1; i < lnr.getLineNumber(); i++) {
                bw.newLine();
            }
            bw.write("<events><event><symbol>ATM</symbol><price>55.6</price><volume>100</volume></event>,");
            bw.newLine();
            bw.write("<event><symbol>IBM</symbol><price>55.6</price><volume>100</volume></event>,");
            bw.newLine();
            bw.write("<event><symbol>IBM</symbol><price>55.6</price><volume>100</volume></event></events>");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();
            lnr.close();
        } catch (IOException e) {
            log.info("error" , e);

        }
        log.info("Starting runtime for the 1st time.");
        siddhiAppRuntime.start();


        byte[] snapshot = siddhiAppRuntime.snapshot();

        siddhiAppRuntime.shutdown();
        try {
            File f = new File("Files/text.xml");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            LineNumberReader  lnr = new LineNumberReader(new FileReader(f));
            for (int i  = 1; i < lnr.getLineNumber(); i++) {
                bw.newLine();
            }
            bw.write("<events><event><symbol>ATM</symbol><price>55.6</price><volume>100</volume></event>,");
            bw.newLine();
            bw.write("<event><symbol>IBM</symbol><price>55.6</price><volume>100</volume></event>,");
            bw.newLine();
            bw.write("<event><symbol>IBM</symbol><price>55.6</price><volume>100</volume></event></events>");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();
            lnr.close();
        } catch (IOException e) {
            log.info("error" , e);

        }
        siddhiAppRuntime.restore(snapshot);

        siddhiAppRuntime.start();
        // siddhiAppRuntime.restoreLastRevision();



        //assert event count
        AssertJUnit.assertEquals("Number of events", 10, count.get());
        siddhiAppRuntime.shutdown();
    }


    //########################################################################

    @Test
    public void testJsonInputDefaultMappingwithpaused() throws InterruptedException {
        log.info("______________Test to read the files and their contents under the given folder______________");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='file', folder.path='Files', file.extension='json', @map(type='json')) " +
                "define stream FooStream (symbol string,  price float,  volume int); " +
                "define stream BarStream (symbol string,  price float,  volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";
        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {
            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    int n = count.getAndIncrement();
                    switch (n) {
                        case 0:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 1:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 2:
                            AssertJUnit.assertEquals(300, event.getData(2));
                            break;
                        case 3:
                            AssertJUnit.assertEquals(100, event.getData(2));
                            break;
                        case 4:
                            AssertJUnit.assertEquals("ATM", event.getData(0));
                            break;
                        case 5:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        case 6:
                            AssertJUnit.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            AssertJUnit.fail("More events received than expected.");
                    }
                }
            }
        });

        try {

            File f = new File("Files/text.json");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            LineNumberReader  lnr = new LineNumberReader(new FileReader(f));
            for (int i  = 0; i < lnr.getLineNumber() - 1; i++) {
                bw.newLine();
            }

            bw.write("[{\"event\":{\"symbol\":\"ATM\", \"price\":55.6, \"volume\":100}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"IBM\", \"price\":55.6, \"volume\":200}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"IBM\", \"price\":55.6, \"volume\":300}}]");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();
            lnr.close();
        } catch (IOException e) {
            log.info("error" , e);

        }
        log.info("Starting runtime for the 1st time.");
        siddhiAppRuntime.start();

        Thread.sleep(1000);
        //Getting snapshot
        byte[] snapshot = siddhiAppRuntime.snapshot();
        Thread.sleep(1000);
        //assert event count
        //SiddhiTestHelper.waitForEvents(2000, 8, count, 30000);
        AssertJUnit.assertEquals("Number of events", 3 , count.get());
        // Siddhi app shutdown
        siddhiAppRuntime.shutdown();
        try {
            File f = new File("Files/text.json");
            FileWriter fw = new FileWriter(f, true);
            BufferedWriter bw = new BufferedWriter(fw);
            LineNumberReader  lnr = new LineNumberReader(new FileReader(f));
            for (int i  = 1; i < lnr.getLineNumber(); i++) {
                bw.newLine();
            }
            bw.write("[{\"event\":{\"symbol\":\"wso2\", \"price\":55.6, \"volume\":400}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"qwe\", \"price\":55.6, \"volume\":500}}, ");
            bw.newLine();
            bw.write("{\"event\":{\"symbol\":\"ert\", \"price\":55.6, \"volume\":600}}]");
            //byte[] snapshot = siddhiAppRuntime.snapshot();
            bw.newLine();
            bw.flush();
            lnr.close();
        } catch (IOException e) {
            log.info("error" , e);

        }

        // Restoring snapshot
        siddhiAppRuntime.restore(snapshot);

        // Restarting runtime
        siddhiAppRuntime.start();

        log.info(count);
        //SiddhiTestHelper.waitForEvents(2000, 11, count, 30000);
        //assert event count
        AssertJUnit.assertEquals("Number of events", 10, count.get());
        // Siddhi app shutdown
        siddhiAppRuntime.shutdown();
    }*/
}
