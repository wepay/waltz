package com.wepay.waltz.storage.server.internal;

import com.wepay.waltz.common.util.Cli;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class WaltzStorageCli extends Cli {
    private String configPath;

    public WaltzStorageCli(String[] args) throws Exception {
        super(args);
    }

    @Override
    protected void processCmd(CommandLine cmd) {
        int argLength = cmd.getArgList().size();

        if (argLength == 0) {
            printErrorAndExit("Missing Waltz storage config path.");
        }

        this.configPath = cmd.getArgList().get(argLength - 1);
    }

    @Override
    protected void configureOptions(Options options) { }

    @Override
    protected String getUsage() {
        return "<waltz_storage_app> <config_path>";
    }

    public String getConfigPath() {
        return configPath;
    }
}
