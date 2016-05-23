package ch.uzh.ifi.adse.twitter.analysis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.amazonaws.services.lambda.runtime.Context;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class AnalysisFunctionHandlerTest {

    private static List<Long> input;

    @BeforeClass
    public static void createInput() throws IOException {
        // TODO: set up your sample input object here.

        input = new ArrayList<Long>();
        input.add(Long.parseLong("718452593148719104"));
        input.add(Long.parseLong("718452593094070272"));
        input.add(Long.parseLong("718452593001963520"));
        input.add(Long.parseLong("718452592993517568"));
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testAnalysisFunctionHandler() {
        AnalysisFunctionHandler handler = new AnalysisFunctionHandler();
        Context ctx = createContext();

        List<Integer> output = handler.handleRequest(input, ctx);

        // TODO: validate output here if needed.
        if (output != null) {
            System.out.println(output.toString());
        }
    }
}
