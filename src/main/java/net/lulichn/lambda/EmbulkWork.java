package net.lulichn.lambda;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.embulk.EmbulkEmbed;
import org.embulk.config.ConfigLoader;
import org.embulk.config.ConfigSource;
import org.embulk.exec.ExecutionResult;
import org.embulk.input.s3.S3FileInputPlugin;
import org.embulk.output.BigqueryOutputPlugin;
import org.embulk.parser.jsonline.JsonLineParserPlugin;
import org.embulk.plugin.InjectedPluginSource;
import org.embulk.spi.InputPlugin;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.ParserPlugin;

import java.io.File;

public class EmbulkWork {
    public String embulk(String key) {
        EmbulkEmbed.Bootstrap bootstrap = new EmbulkEmbed.Bootstrap();
        bootstrap.addModules(
                new S3InputModule(),
                new BigQueryOutputModule(),
                new JsonLineParserModule());


        EmbulkEmbed embulk = bootstrap.initializeCloseable();

        String resultMessage = "";
        try {
            ConfigLoader loader = embulk.newConfigLoader();
            ConfigSource config = loader.fromYamlFile(new File("config.yml"));
            if (key != null) {
                config.getNested("in").set("path_prefix", key);
            }

            ExecutionResult result =  embulk.run(config);
            resultMessage = result.toString();
        } catch (Exception e) {
            e.printStackTrace();
            resultMessage = e.toString();
        } finally {
            embulk.destroy();
        }

        return resultMessage;
    }

    static class S3InputModule implements Module {
        @Override
        public void configure(Binder binder) {
            InjectedPluginSource.registerPluginTo(binder, InputPlugin.class, "s3", S3FileInputPlugin.class);
        }
    }

    static class BigQueryOutputModule implements Module {
        @Override
        public void configure(Binder binder) {
            InjectedPluginSource.registerPluginTo(binder, OutputPlugin.class, "bigquery", BigqueryOutputPlugin.class);
        }
    }

    static class JsonLineParserModule implements Module {
        @Override
        public void configure(Binder binder) {
            InjectedPluginSource.registerPluginTo(binder, ParserPlugin.class, "jsonl", JsonLineParserPlugin.class);
        }
    }
}
