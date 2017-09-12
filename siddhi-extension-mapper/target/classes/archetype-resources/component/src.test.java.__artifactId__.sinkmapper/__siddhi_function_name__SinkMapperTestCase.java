package ${package}.sinkmapper;

import org.apache.log4j.Logger;
import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.NoSuchAttributeException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.InMemorySink;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
public class ${siddhi_function_name}SinkMapperTestCase {
    private static final Logger log = Logger.getLogger(${siddhi_function_name}SinkMapperTestCase.class);
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
    }
    //    from FooStream
    //    select symbol,price,volume
    //    publish inMemory options ("topic", "{{symbol}}")
    //    map csv

    @Test
    public void testCSVSinkmapperDefaultMappingWithSiddhiQL() throws InterruptedException {
        log.info("____________Test default csv mapping with SiddhiQL___________");
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);

            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2" , 55.645f , 100L});
        stockStream.send(new Object[]{"IBM" , 75f , 100L});
        stockStream.send(new Object[]{"WSO2" , 57.6f , 100L});
        stockStream.send(new Object[]{"IBM" , null , 57L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!" , 2  , wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!" , 2 , ibmCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!" , "WSO2,55.645,100" + "\n"
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "IBM,75.0,100" + "\n"
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "WSO2,57.6,100" + "\n"
                , onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "IBM,null,57" + "\n"
                , onMessageList.get(3).toString());
        executionPlanRuntime.shutdown();

        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithSiddhiQL2() throws InterruptedException {
        log.info("___________Test default csv mapping with SiddhiQL for events_____________");
        AtomicInteger companyCount = new AtomicInteger(0);
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                companyCount.incrementAndGet();

            }

            @Override
            public String getTopic() {
                return "company";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='company', @map(type='csv')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        Event wso2Event = new Event(System.currentTimeMillis() , new Object[]{"WSO2#@$" , 55.6f , 100L});
        Event ibmEvent = new Event(System.currentTimeMillis() , new Object[]{"IBM" , 75.6f , 100L});
        stockStream.send(new Event[]{wso2Event , ibmEvent});
        stockStream.send(new Object[]{null , null , 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!" , 2 , companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!" , "WSO2#@$,55.6,100" + "\n" +
                        "IBM,75.6,100" + "\n"
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "null,null,100" + "\n"
                , onMessageList.get(1).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithNullElementSiddhiQL() throws InterruptedException {
        log.info("___________Test default csv mapping with null elements____________");
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();


            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, null});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,55.6,null" + "\n"
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,75.6,100" + "\n"
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,57.6,100" + "\n"
                , onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperDefaultMappingWithDelimiter() throws InterruptedException {
        log.info("____Test default csv mapping with SiddhiQL. Here, CSV delimeter is being provided for mapping.____");
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                //wso2Count.incrementAndGet();
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                //ibmCount.incrementAndGet();
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv', delimiter=\"-\")) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2" , 55.645f , 100L});
        stockStream.send(new Object[]{"IBM" , 75f , 100L});
        stockStream.send(new Object[]{"WSO2" , 57.6f , 100L});
        stockStream.send(new Object[]{"IBM" , null , 57L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!" , 2  , wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!" , 2 , ibmCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!" , "WSO2-55.645-100" + "\n"
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "IBM-75.0-100" + "\n"
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "WSO2-57.6-100" + "\n"
                , onMessageList.get(2).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "IBM-null-57" + "\n"
                , onMessageList.get(3).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }


    //    from FooStream
    //    select symbol,price
    //    publish inMemory options ("topic", "{{symbol}}")
    //    map csv custom

    @Test
    public void testCSVOutputCustomMappingWithoutCSVDelimiter() throws InterruptedException {
        log.info("__________Test custom csv mapping with SiddhiQL___________");
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv', @payload(" +
                "\"{{symbol}},{{price}}\")))" +
                "define stream BarStream (symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom csv
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,55.6" + "\n"
                ,  onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,75.6" + "\n"
                , onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,57.6" + "\n"
                , onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();


        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperCustomMappingWithEvents() throws InterruptedException {
        log.info("Test default csv mapping with SiddhiQL for multiple events");
        AtomicInteger companyCount = new AtomicInteger(0);
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                companyCount.incrementAndGet();

            }

            @Override
            public String getTopic() {
                return "company";
            }
        };
        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "" +
                "@App:name('TestExecutionPlan')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='company', @map(type='csv' ," +
                "@payload(\"{{symbol}},{{price}}\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        Event wso2Event = new Event(System.currentTimeMillis() , new Object[]{"WSO2#@$" , 55.6f , 100L});
        Event ibmEvent = new Event(System.currentTimeMillis() , new Object[]{"IBM" , 75.6f , 100L});
        stockStream.send(new Event[]{wso2Event , ibmEvent});
        stockStream.send(new Object[]{null , null , 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!" , 2 , companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!" , "WSO2#@$,55.6" + "\n" + "IBM,75.6" + "\n"
                , onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!" , "null,null" + "\n"
                , onMessageList.get(1).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }
    @Test
    public void testCSVtputCustomMappingWithCustomAttributes() throws InterruptedException {
        log.info("Test custom csv mapping with SiddhiQL for CSV attributes");
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (id int, symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv', @payload(" +
                "\"id=''{{id}}'',value=''{{price}}'',Symbol={{symbol}},"
                + "company=''{{symbol}}'',Price={{price}}\"))) " +
                "define stream BarStream (id int, symbol string, price float, volume long); ";
        String query = "" +
                "from FooStream " +
                "select id, symbol, price, volume " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{1, "WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{2, "IBM", 75.6f, 100L});
        stockStream.send(new Object[]{3, "WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 2, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom csv
        AssertJUnit.assertEquals("Incorrect mapping!", "id='1',value='55.6',Symbol=WSO2," +
                "company='WSO2',Price=55.6" + "\n", onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "id='2',value='75.6',Symbol=IBM," +
                "company='IBM',Price=75.6" + "\n", onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "id='3',value='57.6',Symbol=WSO2," +
                "company='WSO2',Price=57.6" + "\n", onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVOutputCustomMappingWithoutCustomattribute() throws InterruptedException {
        log.info("Test custom csv mapping with SiddhiQL. Here, payload element is missing");
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                onMessageList.add(msg);
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', " +
                "@map(type='csv')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, wso2Count.get());
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 1, ibmCount.get());
        //assert custom csv
        AssertJUnit.assertEquals("Incorrect mapping!", "WSO2,55.6,100" + "\n",
                onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "IBM,75.6,100" + "\n", onMessageList.get(1).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
        siddhiManager.shutdown();
    }

    @Test
    public void testCSVSinkmapperCustomMappingWithNullAttributes() throws InterruptedException {
        log.info("Test default csv mapping with null attribute");
        AtomicInteger companyCount = new AtomicInteger(0);
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberCompany = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                companyCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "company";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberCompany);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='company', @map(type='csv', " +
                "@payload(\"{{price}},{{volume}},{{symbol}}\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{null, 56, 100L});
        stockStream.send(new Object[]{"WSO2", null, 100L});
        stockStream.send(new Object[]{"WSO2", 56, null});
        Thread.sleep(100);

        //assert event count
        AssertJUnit.assertEquals("Incorrect number of events consumed!", 3, companyCount.get());
        //assert default mapping
        AssertJUnit.assertEquals("Incorrect mapping!", "56,100,null" + "\n",
                onMessageList.get(0).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "null,100,WSO2" + "\n",
                onMessageList.get(1).toString());
        AssertJUnit.assertEquals("Incorrect mapping!", "56,null,WSO2" + "\n",
                onMessageList.get(2).toString());
        executionPlanRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberCompany);
        siddhiManager.shutdown();
    }

    //    from FooStream
    //    select symbol,price
    //    publish inMemory options ("topic", "{{symbol}}")
    //    map csv custom
    @Test(expectedExceptions = NoSuchAttributeException.class)
    public void testNoSuchAttributeExceptionForCSVOutputMapping() throws InterruptedException {
        log.info("Test for non existing attribute in csv mapping with SiddhiQL - expects NoSuchAttributeException");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='csv', @payload(" +
                "\"{{non-exist}},{{price}}\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = executionPlanRuntime.getInputHandler("FooStream");

        executionPlanRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
        siddhiManager.shutdown();
    }
}
