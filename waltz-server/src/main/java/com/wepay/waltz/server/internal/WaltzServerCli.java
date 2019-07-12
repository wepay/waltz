package com.wepay.waltz.server.internal;

import com.wepay.waltz.common.util.Cli;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class WaltzServerCli extends Cli {
    private String configPath;

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

    public String getConfigPath() {
        return configPath;
    }
}
