package ${package}.function;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.exception.SiddhiAppRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.Attribute;

import java.util.Map;

/**
 * This is a sample class-level comment, explaining what the Sink extension class does.
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter",
 *                               description= "The description of the first parameter",
 *                               type = "Supported parameter types.
 *                                      eg:{DataType.STRING, DataType.INT, DataType.LONG etc}",
 *                               dynamic= "true/false
 *                                      (if parameter depends on each event then it is a dynamic parameter.)",
 *                               optional= "true/false, defaultValue = if it is optional then assign a default value
 *                               according to the type."),
 * {@literal @}Parameter(name = "The name of the second parameter",
 *                               description= "The description of the second parameter",
 *                               type= "Supported parameter types."
 *                                      eg:{DataType.STRING, DataType.INT, DataType.LONG etc},
 *                               dynamic=true/false ,
 *                               optional= "true/false ,
 *                               defaultValue= if it is optional then assign a default value
 *                               according to the type."
 *                               ),
 * },
 * //If Sink system configurations will need then
 * systemParameters = {
 * {@literal @}SystemParameter(name = "The name of the first system parameter",
 *                                      description="The description of the first system parameter." ,
 *                                      defaultValue = "The default value of the system parameter.",
 *                                      possibleParameter="The possible value of the system parameter.",
 *                               ),
 * },
 * examples = {
 * {@literal @}Example(syntax= "sample query with Sink annotation that explain how extension use in Siddhi."
 *                     description =" The description of the given example's query."
 *                      ),
 * }
 * )
 * </code></pre>
 */
@Extension(
        name = "total",
        namespace = "custom",
        description = "Returns the total volume  of event's volume1 & volume2.",
        parameters = {},
        returnAttributes = @ReturnAttribute(
                description = "Returns the total volume.",
                type = {DataType.LONG, DataType.DOUBLE}),
        examples = @Example(
                syntax = "@info(name = 'query') " +
                        "from cseEventStream " +
                        "select symbol , custom:plus(volume1,volume2) as totalCount " +
                        "insert into Output;",
                description = "This will produce total volume to each event's volume1 & volume2"
        )
)

public class ${siddhi_function_name}CustomFunctionExtension extends FunctionExecutor {
    private static final Logger log = Logger.
        getLogger(${siddhi_function_name}CustomFunctionExtension.class);

    private Attribute.Type returnType;

    /**
     * The initialization method for {@link CustomFunctionExtension},
     * which will be called before other methods and validate
     * the all configuration and getting the intial values.
     * @param attributeExpressionExecutors are the executors of each attributes in the function
     * @param configReader                 this hold the {@link FunctionExecutor} extensions configuration reader.
     * @param siddhiAppContext             Siddhi app runtime context
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors,
                        ConfigReader configReader, SiddhiAppContext siddhiAppContext) {

        // In this example, CustomFunctionExtension is checked the parameter's type according the task,
        // Appropriately can change this implementation.
        for (ExpressionExecutor expressionExecutor : attributeExpressionExecutors) {
            Attribute.Type attributeType = expressionExecutor.getReturnType();
            if (attributeType == Attribute.Type.DOUBLE) {
                returnType = attributeType;

            } else if ((attributeType == Attribute.Type.STRING) || (attributeType == Attribute.Type.BOOL)) {
                throw new SiddhiAppRuntimeException("Plus cannot have parameters with types String or Bool");
            } else {
                returnType = Attribute.Type.LONG;
            }
        }

    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are more than one function parameter
     *
     * @param data the runtime values of function parameters
     * @return the function result
     */
    @Override
    protected Object execute(Object[] data) {
        // in this example,the logic is written to find the total in each event with more than one function parameter.
        // Change this implementation according to the task
        if (returnType == Attribute.Type.DOUBLE) {
            double total = 0;
            for (Object aObj : data) {
                total += Double.parseDouble(String.valueOf(aObj));
            }
            return total;
        } else {
            long total = 0;
            for (Object aObj : data) {
                total += Long.parseLong(String.valueOf(aObj));
            }
            return total;
        }
    }

    /**
     * The main execution method which will be called upon event arrival
     * when there are zero or one function parameter
     *
     * @param data null if the function parameter count is zero or
     *             runtime data value of the function parameter
     * @return the function result
     */
    @Override
    protected Object execute(Object data) {
        // in this example,the logic is written to find the total in each event with more than one function parameter.
        // Change this implementation according to the task
        if (returnType == Attribute.Type.DOUBLE) {
            return Double.parseDouble(String.valueOf(data));
        } else {
            return Long.parseLong(String.valueOf(data));
        }
    }

    /**
     * return a Class object that represents the formal return type of the method represented by this Method object.
     *
     * @return the return type for the method this object represents
     */
    @Override
    public Attribute.Type getReturnType() {
        // Change this implementation according to the task
        return returnType;
    }


    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {

    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {

    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an map
     */
    @Override
    public Map<String, Object> currentState() {
        return null;
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param map the stateful objects of the processing element as a map.
     *              This is the same map that is created upon calling currentState() method.
     */
    @Override
    public void restoreState(Map<String, Object> map) {

    }
}
