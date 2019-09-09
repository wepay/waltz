package com.wepay.waltz.common.util;

import com.wepay.waltz.exception.SubCommandFailedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract class for implementing command line processing with
 * Apache Commons CLI library.
 *
 * See <a href="https://commons.apache.org/proper/commons-cli/usage.html">usage</a>
 * for references.
 */
public abstract class Cli {
    private HelpFormatter formatter = new HelpFormatter();
    private CommandLineParser parser = new DefaultParser();
    private Options options = new Options();
    private CommandLine cmd;

    protected Cli(String[] args) {
        configureOptions();
        parseArgs(args);
    }

    /**
     * Configure default "-h/--help" flag. Allow additional options to be defined.
     */
    private void configureOptions() {
        Option helpOption = Option.builder("h").longOpt("help").desc("Display help message").build();
        options.addOption(helpOption);

        configureOptions(options);
    }

    /**
     * Add additional {@link Option} instances to the {@link Options} collection.
     */
    protected abstract void configureOptions(Options options);

    /**
     * Build and return command usage based on command descriptions and options.
     * @param descriptions list of description paragraphs
     * @param options command options
     * @return usage
     */
    protected static String buildUsage(List<String> descriptions, Options options) {
        return buildUsage(descriptions, options, "");
    }

    /**
     * Build and return command usage based on command descriptions and options.
     * @param descriptions list of description paragraphs
     * @param options command options
     * @param requiredFile required file name
     * @return usage
     */
    protected static String buildUsage(List<String> descriptions, Options options, String requiredFile) {
        StringBuilder sb = new StringBuilder();
        for (String desc: descriptions) {
            sb.append(desc);
            sb.append("\n\t");
        }
        String description = sb.toString().trim();
        return buildUsage("", description, options, requiredFile);
    }

    /**
     * Build and return command usage based on command name, description and options.
     * @param cmdName command name
     * @param description command description
     * @param options command options
     * @return usage
     */
    protected static String buildUsage(String cmdName, String description, Options options) {
        return buildUsage(cmdName, description, options, "");
    }

    /**
     * Build and return command usage based on command name, description and options.
     * @param cmdName command name
     * @param description command description
     * @param options command options
     * @param requiredFile required file name
     * @return usage
     */
    protected static String buildUsage(String cmdName, String description, Options options, String requiredFile) {
        StringBuilder sb = new StringBuilder();
        sb.append(cmdName);
        options.getOptions().stream()
                .filter(option -> !option.isRequired())
                .forEach(option -> sb.append(String.format(" [--%s | -%s]", option.getLongOpt(), option.getOpt())));
        options.getOptions().stream()
                .filter(option -> option.isRequired())
                .forEach(option -> sb.append(String.format(" <--%s | -%s>", option.getLongOpt(), option.getOpt())));
        if (!requiredFile.isEmpty()) {
            sb.append(" <" + requiredFile + ">");
        }
        sb.append("\n\ndescription:\t");
        sb.append(description);
        sb.append("\n\noptions:");
        return sb.toString();
    }

    /**
     * Parse the command line arguments into a {@link CommandLine} object
     * and process it.
     */
    private void parseArgs(String[] args) {
        if (args == null) {
            printErrorAndExit("Cannot process args if null");
        }

        // check [-h | --help] before parser.parse() because it would fail
        // if any required parameter is missing, thus cannot print usage
        List<String> argList = Arrays.asList(args);
        if (argList.contains("-h") || argList.contains("--help")) {
            printUsageAndExit();
        }

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            printErrorAndExit(e.getMessage());
        }
    }

    /**
     * Process the default "-h/--help" flag in the {@link CommandLine} instance.
     * Allow additional options to be processed.
     */
    public void processCmd() throws SubCommandFailedException {
        processCmd(cmd);
    }

    /**
     * Process the {@link CommandLine} object, can be used to validate inputs
     * and store properties for later use.
     */
    protected abstract void processCmd(CommandLine cmd) throws SubCommandFailedException;

    /**
     * Returns the Usage message of the program.
     */
    protected abstract String getUsage();

    /**
     * Helper method for exiting and printing the usage of the program.
     */
    protected void printUsageAndExit() {
        formatter.printHelp(getUsage(), options);
        System.exit(0);
    }

    /**
     * Helper method for exiting the program when error occurs:
     * print the errorMessage.
     */
    protected void printErrorAndExit(@Nullable String errorMessage) {
        printError(errorMessage);
        System.exit(1);
    }

    /**
     * Helper method for printing the errorMessage.
     */
    protected void printError(@Nullable String errorMessage) {
        if (errorMessage != null && !errorMessage.isEmpty()) {
            System.err.println("Error: " + errorMessage);
        }
    }

    /**
     * Get command line options.
     * @return options
     */
    protected Options getOptions() {
        return options;
    }
}
