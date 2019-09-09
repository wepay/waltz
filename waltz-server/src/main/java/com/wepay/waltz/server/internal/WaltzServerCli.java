package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.util.Cli;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Extends {@link Cli} to parse {@link com.wepay.waltz.server.WaltzServer} CLI arguments.
 */
public class WaltzServerCli extends Cli {
    private String configPath;

    /**
     * Class constructor.
     * @param args CLI arguments.
     * @throws Exception thrown in case of argument parsing exception.
     */
    public WaltzServerCli(String[] args) throws Exception {
        super(args);
    }

    @Override
    protected void processCmd(CommandLine cmd) {
        int argLength = cmd.getArgList().size();

        if (argLength == 0) {
            printErrorAndExit("Missing Waltz server config path.");
        }

        this.configPath = cmd.getArgList().get(argLength - 1);
    }

    @Override
    protected void configureOptions(Options options) { }

    @Override
    protected String getUsage() {
        return "<waltz_server_app> <config_path>";
    }

    /**
     * Returns WaltzServer CLI config path.
     * @return WaltzServer CLI config path.
     */
    public String getConfigPath() {
        return configPath;
    }
}
