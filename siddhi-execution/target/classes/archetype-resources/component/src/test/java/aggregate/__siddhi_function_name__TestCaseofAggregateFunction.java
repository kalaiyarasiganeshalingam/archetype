package ${package}.aggregate;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;


public class ${siddhi_function_name}TestCaseofAggregateFunction {
    private static final Logger log = Logger.
        getLogger(${siddhi_function_name}TestCaseofAggregateFunction.class);
    private Object count;

    @Before
    public void init() {
        count = 0;
    }
    @Test
    public void testAggregateFunction1() throws InterruptedException {
        log.info("_________AggregateFunction TestCase__________");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "define stream pizzaOrder (orderNo int); ";
        String query = "" +
                "@info(name = 'query') " +
                "from pizzaOrder#window.time(1 sec)" +
                "select custom:aggregator(orderNo) as totalOrders" +
                " insert into OutMediationStream;";
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count = event.getData(0);
                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("pizzaOrder");
        executionPlanRuntime.start();
        Thread.sleep(100);
        inputHandler.send(new Object[]{10863690});
        Thread.sleep(100);
        inputHandler.send(new Object[]{7868});
        Thread.sleep(100);
        inputHandler.send(new Object[]{823863});
        Thread.sleep(100);
        inputHandler.send(new Object[]{8368});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
        Assert.assertEquals(Long.valueOf(4), count);
    }
    @Test
    public void testAggregateFunction2() throws InterruptedException {
        log.info("_________AggregateFunction TestCase with events__________");
        SiddhiManager siddhiManager = new SiddhiManager();

        String streams = "define stream pizzaOrder (orderNo int); ";
        String query = "" +
                "@info(name = 'query') " +
                "from pizzaOrder#window.time(1 sec)" +
                "select custom:aggregator(orderNo) as totalOrders" +
                " insert into OutMediationStream;";
        SiddhiAppRuntime executionPlanRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        executionPlanRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    count = event.getData(0);
                }
            }
        });
        InputHandler inputHandler = executionPlanRuntime.getInputHandler("pizzaOrder");
        executionPlanRuntime.start();
        Thread.sleep(100);
        inputHandler.send(new Object[]{10863690});
        Thread.sleep(100);
        inputHandler.send(new Object[]{7868});
        Thread.sleep(100);
        inputHandler.send(new Object[]{823863});
        Thread.sleep(100);
        inputHandler.send(new Object[]{8368});
        Thread.sleep(100);
        executionPlanRuntime.shutdown();
        Assert.assertEquals(Long.valueOf(4), count);
    }
}
