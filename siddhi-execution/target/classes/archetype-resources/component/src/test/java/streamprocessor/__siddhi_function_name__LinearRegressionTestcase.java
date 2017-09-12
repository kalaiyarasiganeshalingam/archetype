package ${package}.streamprocessor;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class ${siddhi_function_name}LinearRegressionTestcase {

    protected static SiddhiManager siddhiManager;
    private static Logger log = Logger.getLogger(${siddhi_function_name}LinearRegressionTestcase.class);
    private int count;
    private double betaZero,  betaTwo,  forecastY;
    private boolean outlier;

    @Before
    public void init() {
        count = 0;
    }

    @Test
    public void linearRegressionTestcase() throws InterruptedException {


        log.info("Simple Regression TestCase");

        siddhiManager = new SiddhiManager();
        String streams = "define stream InputStream (y int,  x int);";

        String query = ("@info(name = 'query1') from InputStream#timeseries:regress(1,  100,  0.95,  y,  x) "
                + "select * "
                + "insert into OutputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                count = count + inEvents.length;
                betaZero = (Double) inEvents[inEvents.length - 1].getData(3);
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();

        log.info("Current time" + System.currentTimeMillis());

        inputHandler.send(new Object[]{2500.00, 17.00});
        inputHandler.send(new Object[]{2600.00, 18.00});
        inputHandler.send(new Object[]{3300.00, 31.00});
        inputHandler.send(new Object[]{2475.00, 12.00});
        inputHandler.send(new Object[]{2313.00, 8.00});
        inputHandler.send(new Object[]{2175.00, 26.00});
        inputHandler.send(new Object[]{600.00, 14.00});
        inputHandler.send(new Object[]{460.00, 3.00});
        inputHandler.send(new Object[]{240.00, 1.00});
        inputHandler.send(new Object[]{200.00, 10.00});
        inputHandler.send(new Object[]{177.00, 0.00});
        inputHandler.send(new Object[]{140.00, 6.00});
        inputHandler.send(new Object[]{117.00, 1.00});
        inputHandler.send(new Object[]{115.00, 0.00});
        inputHandler.send(new Object[]{2600.00, 19.00});
        inputHandler.send(new Object[]{1907.00, 13.00});
        inputHandler.send(new Object[]{1190.00, 3.00});
        inputHandler.send(new Object[]{990.00, 16.00});
        inputHandler.send(new Object[]{925.00, 6.00});
        inputHandler.send(new Object[]{365.00, 0.00});
        inputHandler.send(new Object[]{302.00, 10.00});
        inputHandler.send(new Object[]{300.00, 6.00});
        inputHandler.send(new Object[]{129.00, 2.00});
        inputHandler.send(new Object[]{111.00, 1.00});
        inputHandler.send(new Object[]{6100.00, 18.00});
        inputHandler.send(new Object[]{4125.00, 19.00});
        inputHandler.send(new Object[]{3213.00, 1.00});
        inputHandler.send(new Object[]{2319.00, 38.00});
        inputHandler.send(new Object[]{2000.00, 10.00});
        inputHandler.send(new Object[]{1600.00, 0.00});
        inputHandler.send(new Object[]{1394.00, 4.00});
        inputHandler.send(new Object[]{935.00, 4.00});
        inputHandler.send(new Object[]{850.00, 0.00});
        inputHandler.send(new Object[]{775.00, 5.00});
        inputHandler.send(new Object[]{760.00, 6.00});
        inputHandler.send(new Object[]{629.00, 1.00});
        inputHandler.send(new Object[]{275.00, 6.00});
        inputHandler.send(new Object[]{120.00, 0.00});
        inputHandler.send(new Object[]{2567.00, 12.00});
        inputHandler.send(new Object[]{2500.00, 28.00});
        inputHandler.send(new Object[]{2350.00, 21.00});
        inputHandler.send(new Object[]{2317.00, 3.00});
        inputHandler.send(new Object[]{2000.00, 12.00});
        inputHandler.send(new Object[]{715.00, 1.00});
        inputHandler.send(new Object[]{660.00, 9.00});
        inputHandler.send(new Object[]{650.00, 0.00});
        inputHandler.send(new Object[]{260.00, 0.00});
        inputHandler.send(new Object[]{250.00, 1.00});
        inputHandler.send(new Object[]{200.00, 13.00});
        inputHandler.send(new Object[]{180.00, 6.00});
        Thread.sleep(100);

        Assert.assertEquals("No of events: ", 50, count);
        Assert.assertEquals("Beta0: ", 573.1418421169493, betaZero, 573.1418421169493 - betaZero);

        executionPlanRuntime.shutdown();
    }
    @Test
    public void multipleRegressionTest() throws Exception {
        log.info("Multiple Regression TestCase");

        siddhiManager = new SiddhiManager();
        String streams = "define stream InputStream (a int,  b int,  c int,  d int,  e int);";

        String query = ("@info(name = 'query2') from InputStream#timeseries:regress(a,  c,  b,  e) "
                + "select * "
                + "insert into OutputStream;");
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        executionPlanRuntime.addCallback("query2",  new QueryCallback() {
            @Override
            public void receive(long timeStamp,  Event[] inEvents,
                                Event[] removeEvents) {
                EventPrinter.print(timeStamp,  inEvents,  removeEvents);
                count = count + inEvents.length;
                betaTwo = (Double) inEvents[inEvents.length - 1].getData(8);
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("InputStream");
        executionPlanRuntime.start();
        log.info(System.currentTimeMillis());

        inputHandler.send(new Object[]{ 3300 ,  104 ,  22 ,  80 ,  3 });
        inputHandler.send(new Object[]{ 2600 ,  66 ,  39 ,  69 ,  3 });
        inputHandler.send(new Object[]{ 2500 ,  73 ,  63 ,  116 ,  5 });
        inputHandler.send(new Object[]{ 2475 ,  50 ,  23 ,  64 ,  21 });
        inputHandler.send(new Object[]{ 2313 ,  58 ,  70 ,  53 ,  8 });
        inputHandler.send(new Object[]{ 2175 ,  100 ,  87 ,  89 ,  4 });
        inputHandler.send(new Object[]{ 600 ,  38 ,  15 ,  45 ,  10 });
        inputHandler.send(new Object[]{ 460 ,  21 ,  11 ,  32 ,  3 });
        inputHandler.send(new Object[]{ 240 ,  18 ,  24 ,  26 ,  2 });
        inputHandler.send(new Object[]{ 200 ,  33 ,  14 ,  96 ,  6 });
        inputHandler.send(new Object[]{ 177 ,  10 ,  5 ,  18 ,  7 });
        inputHandler.send(new Object[]{ 140 ,  22 ,  19 ,  56 ,  3 });
        inputHandler.send(new Object[]{ 117 ,  3 ,  2 ,  1 ,  0 });
        inputHandler.send(new Object[]{ 115 ,  2 ,  4 ,  3 ,  0 });
        inputHandler.send(new Object[]{ 2600 ,  75 ,  53 ,  64 ,  7 });
        inputHandler.send(new Object[]{ 1907 ,  73 ,  50 ,  100 ,  14 });
        inputHandler.send(new Object[]{ 1190 ,  26 ,  42 ,  61 ,  8 });
        inputHandler.send(new Object[]{ 990 ,  64 ,  42 ,  102 ,  6 });
        inputHandler.send(new Object[]{ 925 ,  26 ,  22 ,  26 ,  5 });
        inputHandler.send(new Object[]{ 365 ,  15 ,  14 ,  30 ,  6 });
        inputHandler.send(new Object[]{ 302 ,  51 ,  95 ,  151 ,  27 });
        inputHandler.send(new Object[]{ 300 ,  39 ,  34 ,  89 ,  6 });
        inputHandler.send(new Object[]{ 129 ,  18 ,  20 ,  22 ,  5 });
        inputHandler.send(new Object[]{ 111 ,  8 ,  1 ,  18 ,  0 });
        inputHandler.send(new Object[]{ 6100 ,  100 ,  90 ,  67 ,  15 });
        inputHandler.send(new Object[]{ 4125 ,  96 ,  55 ,  74 ,  7 });
        inputHandler.send(new Object[]{ 3213 ,  17 ,  39 ,  47 ,  3 });
        inputHandler.send(new Object[]{ 2319 ,  117 ,  78 ,  120 ,  31 });
        inputHandler.send(new Object[]{ 2000 ,  40 ,  36 ,  56 ,  4 });
        inputHandler.send(new Object[]{ 1600 ,  31 ,  50 ,  69 ,  15 });
        inputHandler.send(new Object[]{ 1394 ,  51 ,  83 ,  50 ,  5 });
        inputHandler.send(new Object[]{ 935 ,  21 ,  30 ,  42 ,  3 });
        inputHandler.send(new Object[]{ 850 ,  54 ,  75 ,  38 ,  20 });
        inputHandler.send(new Object[]{ 775 ,  35 ,  9 ,  19 ,  3 });
        inputHandler.send(new Object[]{ 760 ,  36 ,  40 ,  53 ,  14 });
        inputHandler.send(new Object[]{ 629 ,  30 ,  24 ,  43 ,  0 });
        inputHandler.send(new Object[]{ 275 ,  34 ,  33 ,  57 ,  8 });
        inputHandler.send(new Object[]{ 120 ,  5 ,  14 ,  19 ,  2 });
        inputHandler.send(new Object[]{ 2567 ,  42 ,  41 ,  66 ,  8 });
        inputHandler.send(new Object[]{ 2500 ,  81 ,  48 ,  93 ,  5 });
        inputHandler.send(new Object[]{ 2350 ,  92 ,  67 ,  100 ,  3 });
        inputHandler.send(new Object[]{ 2317 ,  12 ,  37 ,  20 ,  4 });
        inputHandler.send(new Object[]{ 2000 ,  40 ,  12 ,  57 ,  9 });
        inputHandler.send(new Object[]{ 715 ,  11 ,  16 ,  36 ,  3 });
        inputHandler.send(new Object[]{ 660 ,  49 ,  14 ,  49 ,  9 });
        inputHandler.send(new Object[]{ 650 ,  15 ,  30 ,  30 ,  4 });
        inputHandler.send(new Object[]{ 260 ,  12 ,  13 ,  14 ,  0 });
        inputHandler.send(new Object[]{ 250 ,  11 ,  2 ,  26 ,  2 });
        inputHandler.send(new Object[]{ 200 ,  50 ,  31 ,  73 ,  3 });
        inputHandler.send(new Object[]{ 180 ,  21 ,  17 ,  26 ,  8 });

        Thread.sleep(100);

        Assert.assertEquals("No of events: ",  50,  count);
        Assert.assertEquals("Beta2: ",  26.665526771748596,  betaTwo,  26.665526771748596 - betaTwo);

        executionPlanRuntime.shutdown();
    }
}
