import java.io.*;

class main {
    private static String modelClassName;
    private static String inputCSVFileName;
    private static String outputCSVFileName;
    private static int skipFirstLine = -1;

    private static void usage() {
        System.out.println("");
        System.out.println("usage:  java [...java args...] main (--header | --noheader) --model modelClassName --input inputCSVFileName --output outputCSVFileName");
        System.out.println("");
        System.out.println("        model class name is something like GBMModel_blahblahblahblah.");
        System.out.println("");
        System.out.println("        inputCSV is the test data set.");
        System.out.println("        Specify --header or --noheader as appropriate.");
        System.out.println("");
        System.out.println("        outputCSV is the prediction data set (one row per test data set).");
        System.out.println("");
        System.exit(1);
    }

    private static void usageHeader() {
        System.out.println("ERROR: One of --header or --noheader must be specified exactly once");
        usage();
    }

    private static void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            String s = args[i];
            if (s.equals("--model")) {
                i++; if (i >= args.length) usage();
                modelClassName = args[i];
            }
            else if (s.equals("--input")) {
                i++; if (i >= args.length) usage();
                inputCSVFileName = args[i];
            }
            else if (s.equals("--output")) {
                i++; if (i >= args.length) usage();
                outputCSVFileName = args[i];
            }
            else if (s.equals("--header")) {
                if (skipFirstLine >= 0) usageHeader();
                skipFirstLine = 1;
            }
            else if (s.equals("--noheader")) {
                if (skipFirstLine >= 0) usageHeader();
                skipFirstLine = 0;
            }
            else {
                System.out.println("ERROR: Bad parameter: " + s);
                usage();
            }
        }

        if (skipFirstLine < 0) {
            usageHeader();
        }

        if (modelClassName == null) {
            System.out.println("ERROR: model not specified");
            usage();
        }

        if (inputCSVFileName == null) {
            System.out.println("ERROR: input not specified");
            usage();
        }

        if (outputCSVFileName == null) {
            System.out.println("ERROR: output not specified");
            usage();
        }
    }

    public static void main(String[] args) throws Exception{
        parseArgs(args);

        water.genmodel.GeneratedModel model;
        model = (water.genmodel.GeneratedModel) Class.forName(modelClassName).newInstance();

        BufferedReader input = new BufferedReader(new FileReader(inputCSVFileName));
        BufferedWriter output = new BufferedWriter(new FileWriter(outputCSVFileName));

        // Print outputCSV column names.
        output.write("predict");
        for (int i = 0; i < model.getNumResponseClasses(); i++) {
            output.write(",");
            output.write(model.getNames()[i]);
        }
        output.write("\n");

        // Loop over inputCSV one row at a time.
        int lineno = 0;
        String line = input.readLine();
        while (line != null) {
            lineno++;
            if (skipFirstLine > 0) {
                // TODO:  compare that these column headers match model.getNames().
                skipFirstLine = 0;
                line = input.readLine();
            }

            // Parse the CSV line.  Don't handle quoted commas.  This isn't a parser test.
            String trimmedLine = line.trim();
            String[] inputColumnsArray = trimmedLine.split(",");
            int numInputColumns = model.getNames().length;
            if (inputColumnsArray.length != numInputColumns) {
                System.out.println("WARNING: Line " + lineno + " has " + inputColumnsArray.length + " columns (expected " + numInputColumns + ")");
                // System.exit(1);
            }

            // Assemble the input values for the row.
            double[] row = new double[inputColumnsArray.length];
            for (int i = 0; i < inputColumnsArray.length; i++) {
                String[] domainValues = model.getDomainValues(i);
                if (domainValues != null) {
                    System.out.println("ERROR: Unimplemented");
                    System.exit(1);
                }
                double value = Double.parseDouble(inputColumnsArray[i]);
                row[i] = value;
            }

            // Do the prediction.
            float[] preds = new float[model.getNumResponseClasses()+1];
            model.predict(row, preds);

            // Emit the result to the output file.
            for (int i = 0; i < preds.length; i++) {
                if (i > 0) {
                    output.write(",");
                }
                output.write(Double.toString(preds[i]));
            }
            output.write("\n");

            // Prepare for next line of input.
            line = input.readLine();
        }

        // Clean up.
        output.close();
        input.close();

        // Predictions were successfully generated.  Calling program can now compare them with something.
        System.exit(0);
    }
}
