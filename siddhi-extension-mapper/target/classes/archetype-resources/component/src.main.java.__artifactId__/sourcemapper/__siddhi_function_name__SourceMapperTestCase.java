package ${package}.sourcemapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.concurrent.atomic.AtomicInteger;


public class ${siddhi_function_name}SourceMapperTestCase {
    private static final Logger log = Logger.getLogger(${siddhi_function_name}SourceMapperTestCase.class);
    private AtomicInteger count = new AtomicInteger();

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    /**
     * Expected input format:
     * WSO2,55.6,100\n
     */
    @Test
    public void testXmlInputDefaultMapping() throws InterruptedException {
        log.info("__________Test case for csv input mapping with default mapping___________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv')) " +
                "define stream FooStream (symbol string, price float, volume int); " +
                "define stream BarStream (symbol string, price float, volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(55.689f, event.getData(1));
                            org.junit.Assert.assertEquals(" ", event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals(75.0f, event.getData(1));
                            org.junit.Assert.assertEquals("IBM@#$%^*", event.getData(0));
                            break;
                        case 3:
                            org.junit.Assert.assertEquals(" ", event.getData(1));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", " ,55.689,100");
        InMemoryBroker.publish("stock", "IBM@#$%^*,75, ");
        InMemoryBroker.publish("stock", " WSO2,,10");
        InMemoryBroker.publish("stock", " WSO2,55.6,aa");
        InMemoryBroker.publish("stock", " WSO2,abb,10");
        InMemoryBroker.publish("stock", " WSO2,bb,10.6");

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    @org.testng.annotations.Test
    public void testXmlInputDefaultMappingwithDelimiter() throws InterruptedException {
        log.info("__________Test case for csv input mapping with default delimiter__________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv',delimiter=\"-\")) " +
                "define stream FooStream (symbol string, price float, volume int); " +
                "define stream BarStream (symbol string, price float, volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(55.689f, event.getData(1));
                            org.junit.Assert.assertEquals(" ", event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals(75.0f, event.getData(1));
                            org.junit.Assert.assertEquals("IBM@#$%^*", event.getData(0));
                            break;
                        case 3:
                            org.junit.Assert.assertEquals(" ", event.getData(1));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", " -55.689-100");
        InMemoryBroker.publish("stock", "IBM@#$%^*-75- ");
        InMemoryBroker.publish("stock", " WSO2--10");
        InMemoryBroker.publish("stock", " WSO2-55.6-aa");
        InMemoryBroker.publish("stock", " WSO2-abb-10");
        InMemoryBroker.publish("stock", " WSO2-bb-10.6");

        //assert event count
        AssertJUnit.assertEquals("Number of events", 1, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
    @org.testng.annotations.Test
    public void testXmlInputDefaultMappingMultipleEvents() throws InterruptedException {
        log.info("______Test case for csv input mapping with default mapping for multiple events_______");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(55.6f, event.getData(1));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "WSO2,55.6,100\nIBM,75.6,10");

        //assert event count
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
    @org.testng.annotations.Test
    public void testXmlInputDefaultMappingwithfailunknownattribute() throws InterruptedException {
        log.info("________Test case for csv input mapping with failunknownattribute=false________ ");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv',fail.on.unknown.attribute=\"false\")) " +
                "define stream FooStream (symbol string, price float, volume int); " +
                "define stream BarStream (symbol string, price float, volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(55.689f, event.getData(1));
                            org.junit.Assert.assertEquals(" ", event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals(75.0f, event.getData(1));
                            org.junit.Assert.assertEquals("IBM@#$%^*", event.getData(0));
                            break;
                        case 3:
                            org.junit.Assert.assertEquals(null, event.getData(1));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", " ,55.689,100");
        InMemoryBroker.publish("stock", "IBM@#$%^*,75, ");
        InMemoryBroker.publish("stock", " WSO2,,10");

        AssertJUnit.assertEquals("Number of events", 3, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }

    //Custom mapping
    @Test
    public void testXmlInputCustomMapping() throws InterruptedException {
        log.info("__________Test case for csv input mapping with custom mapping using delimeter. " +
                "Here, only one event is sent in a message._____________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv',delimiter=\"-\"," +
                "@attributes(symbol=\"2\",price =\"0\",volume =\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(100L, event.getData(2));
                            org.junit.Assert.assertEquals(29.3f, event.getData(1));
                            org.junit.Assert.assertEquals("WSO2", event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals(243L, event.getData(2));
                            org.junit.Assert.assertEquals(25f, event.getData(1));
                            org.junit.Assert.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "29.3-100-WSO2");
        InMemoryBroker.publish("stock", "25-243-IBM");
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
    @Test
    public void testXmlInputCustomMappingMultipleEvent() throws InterruptedException {
        log.info("__________Test case for csv input mapping with custom mapping." +
                " Here, only one event is sent in a message.___________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv'," +
                "@attributes(symbol=\"2\",price =\"0\",volume =\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(100L, event.getData(2));
                            org.junit.Assert.assertEquals(29.3f, event.getData(1));
                            org.junit.Assert.assertEquals("WSO2", event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals(243L, event.getData(2));
                            org.junit.Assert.assertEquals(25f, event.getData(1));
                            org.junit.Assert.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "29.3,100,WSO2\n25,243,IBM");
        //InMemoryBroker.publish("stock", "nasdaq,75.8,208");
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
    @Test
    public void testXmlInputCustomMappingwithDelimeterandMultipleEvent() throws InterruptedException {
        log.info("_________Test case for csv input mapping with custom mapping. " +
                "Here, only one event is sent in a message.________________");


        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv',delimiter=\"-\"," +
                "@attributes(symbol=\"2\",price =\"0\",volume =\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(100L, event.getData(2));
                            org.junit.Assert.assertEquals(29.3f, event.getData(1));
                            org.junit.Assert.assertEquals("WSO2", event.getData(0));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals(243L, event.getData(2));
                            org.junit.Assert.assertEquals(25f, event.getData(1));
                            org.junit.Assert.assertEquals("IBM", event.getData(0));
                            break;
                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "29.3-100-WSO2\n25-243-IBM");
        //InMemoryBroker.publish("stock", "nasdaq,75.8,208");
        AssertJUnit.assertEquals("Number of events", 2, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
    @org.testng.annotations.Test
    public void testCSVInputCustomMappingwithfailunknownattribute() throws InterruptedException {
        log.info("____________Test case for csv default mapping with fail.on.unknown.attribute______________");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='csv',fail.on.unknown.attribute=\"false\"," +
                "@attributes(symbol=\"2\",price =\"0\",volume =\"1\"))) " +
                "define stream FooStream (symbol string, price float, volume int); " +
                "define stream BarStream (symbol string, price float, volume int); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                //EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            org.junit.Assert.assertEquals(55.689f, event.getData(1));
                            org.junit.Assert.assertEquals(100, event.getData(2));
                            break;
                        case 2:
                            org.junit.Assert.assertEquals(75.0f, event.getData(1));
                            org.junit.Assert.assertEquals("IBM@#$%^*", event.getData(0));
                            break;
                        case 3:
                            org.junit.Assert.assertEquals(null, event.getData(1));
                            break;
                        case 4:
                            org.junit.Assert.assertEquals(null, event.getData(2));
                            break;
                        case 5:
                            org.junit.Assert.assertEquals(null, event.getData(1));
                            break;
                        case 6:
                            org.junit.Assert.assertEquals(null, event.getData(1));
                            break;

                        default:
                            org.junit.Assert.fail();
                    }
                }
            }
        });
        executionPlanRuntime.start();
        InMemoryBroker.publish("stock", "55.689,100, ");
        InMemoryBroker.publish("stock", "75, ,IBM@#$%^*");
        InMemoryBroker.publish("stock", " ,10,WSO2,");
        InMemoryBroker.publish("stock", " 55.6,aa,WSO2");
        InMemoryBroker.publish("stock", " abb,10,WSO2");
        InMemoryBroker.publish("stock", " bb,10.6,WSO2");

        //assert event count
        AssertJUnit.assertEquals("Number of events", 6, count.get());
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
