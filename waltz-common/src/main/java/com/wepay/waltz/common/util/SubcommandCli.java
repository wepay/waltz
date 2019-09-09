package com.wepay.waltz.common.util;

import com.wepay.waltz.exception.SubCommandFailedException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * SubcommandCli is a helper for creating Cli tools with subcommands.
 */
public class SubcommandCli {
    private final boolean useByTest;
    private final Map<String, Subcommand> subcommands;
    private final String[] args;

    /**
     * @param args CLI arguments
     * @param useByTest Whether CLI is used by test
     * @param subcommands A list of supported subcommands
     */
    public SubcommandCli(String[] args, boolean useByTest, List<Subcommand> subcommands) {
        // Don't store a reference to an externally mutable object
        // in the internal representation of SubcommandCli
        this.args = args.clone();
        this.useByTest = useByTest;
        this.subcommands = subcommands
                .stream()
                .collect(Collectors.toMap(Subcommand::getName, item -> item, (u, v) -> {
                    throw new IllegalStateException(String.format("Duplicate key %s", u));
                }, LinkedHashMap::new));
    }

    /**
     * Process the arguments that were passed in as args in the constructor. If
     * no subcommand is available for the specified argument, then print usage
     * information and exit. If SubCommandFailedException throws while processing
     * command, and command is not used by test, exit with code 1.
     */
    @SuppressFBWarnings(value = "DM_EXIT", justification = "shutdown command line")
    public void processCmd() {
        if (this.args == null || this.args.length == 0) {
            printUsageAndExit("Missing subcommand.");
        } else if (!subcommands.containsKey(args[0])) {
            printUsageAndExit("Unknown subcommand: " + args[0]);
        } else {
            Subcommand subcommand = subcommands.get(args[0]);
            String[] subcommandArgs = Arrays.copyOfRange(args, 1, args.length);
            Cli cli = subcommand.cliFunction.apply(subcommandArgs);
            try {
                cli.processCmd();
                System.exit(0);
            } catch (SubCommandFailedException e) {
                cli.printError(e.getMessage());
                if (!useByTest) {
                    System.exit(1);
                }
            }
        }
    }

    /**
     * Helper method for exiting the program when error occurs:
     * print the errorMessage and the usage of the program.
     */
    protected void printUsageAndExit(String errorMessage) {
        StringBuilder sb = new StringBuilder();

        sb.append('\n');

        if (errorMessage != null && !errorMessage.isEmpty()) {
            sb.append("Error: ").append(errorMessage).append("\n\n");
        }

        // Find the length of the longest subcommand so we can make all subcommands print with alignment.
        int maxSubcommandLength = subcommands
                .keySet()
                .stream()
                .max(Comparator.comparingInt(String::length))
                .orElseThrow(() -> new IllegalArgumentException("Subcommands must not be empty"))
                .length();

        sb.append("subcommands:\n\n");

        for (Map.Entry<String, Subcommand> subcommand : subcommands.entrySet()) {
            String subcommandName = subcommand.getKey();
            sb.append("  ");
            // Use string formatter to force all subcommand chunks to be the same width.
            sb.append(String.format("%-" + maxSubcommandLength + "s", subcommandName));
            sb.append("    ");
            sb.append(subcommand.getValue().getDescription());
            sb.append("\n");
        }

        System.out.println(sb);
        System.exit(1);
    }

    public static class Subcommand {
        private final String name;
        private final String description;
        private final Function<String[], Cli> cliFunction;

        /**
         * @param name The name of the subcommand
         * @param description A short, single sentence, description of the subcommand
         * @param cliFunction Provider for the Cli to use when processing the subcommand
         */
        public Subcommand(String name, String description, Function<String[], Cli> cliFunction) {
            this.name = name;
            this.description = description;
            this.cliFunction = cliFunction;
        }

        public String getName() {
            return name;
        }

        public String getDescription() {
            return description;
        }

        public Function<String[], Cli> getCliFunction() {
            return cliFunction;
        }
    }
}
