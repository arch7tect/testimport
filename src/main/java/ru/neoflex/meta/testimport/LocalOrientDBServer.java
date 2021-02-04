package ru.neoflex.meta.testimport;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.*;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent;

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;

public class LocalOrientDBServer implements Closeable {
    private String home;
    protected OServer oServer;

    public LocalOrientDBServer(String home) throws Exception {
        this.home = home;
        this.oServer = OServerMain.create(false);
    }

    protected OServerConfiguration createDefaultServerConfiguration(String dbPath) {
        OServerConfiguration configuration = new OServerConfiguration();
        configuration.network = new OServerNetworkConfiguration();
        configuration.network.protocols = new ArrayList<OServerNetworkProtocolConfiguration>() {{
            add(new OServerNetworkProtocolConfiguration("binary",
                    "com.orientechnologies.orient.server.network.protocol.binary.ONetworkProtocolBinary"));
            add(new OServerNetworkProtocolConfiguration("http",
                    "com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpDb"));
        }};
        configuration.network.listeners = new ArrayList<OServerNetworkListenerConfiguration>() {{
            add(new OServerNetworkListenerConfiguration() {{
                protocol = "binary";
                ipAddress = "0.0.0.0";
                portRange = "2424-2430";
            }});
            add(new OServerNetworkListenerConfiguration() {{
                protocol = "http";
                ipAddress = "0.0.0.0";
                portRange = "2480-2490";
                commands = new OServerCommandConfiguration[]{new OServerCommandConfiguration() {{
                    implementation = "com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent";
                    pattern = "GET|www GET|studio/ GET| GET|*.htm GET|*.html GET|*.xml GET|*.jpeg GET|*.jpg GET|*.png GET|*.gif GET|*.js GET|*.css GET|*.swf GET|*.ico GET|*.txt GET|*.otf GET|*.pjs GET|*.svg";
                    parameters = new OServerEntryConfiguration[]{
                            new OServerEntryConfiguration("http.cache:*.htm *.html", "Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache"),
                            new OServerEntryConfiguration("http.cache:default", "Cache-Control: max-age=120"),
                    };
                }}};
                parameters = new OServerParameterConfiguration[]{
                        new OServerParameterConfiguration("network.http.charset", "UTF-8"),
                        new OServerParameterConfiguration("network.http.jsonResponseError", "true")
                };
            }});
        }};
        configuration.users = new OServerUserConfiguration[]{
                new OServerUserConfiguration("root", "ne0f1ex", "*"),
                new OServerUserConfiguration("admin", "admin", "*"),
        };
        configuration.properties = new OServerEntryConfiguration[]{
                new OServerEntryConfiguration("server.cache.staticResources", "false"),
                new OServerEntryConfiguration("log.console.level", "info"),
                new OServerEntryConfiguration("log.console.level", "info"),
                new OServerEntryConfiguration("log.file.level", "fine"),
                new OServerEntryConfiguration("server.database.path", dbPath),
                new OServerEntryConfiguration("plugin.dynamic", "true"),
        };
        return configuration;
    }

    protected void registerWwwAsStudio() {
        OCallable<Object, String> oCallable = iArgument -> {
            String fileName = "www/" + iArgument;
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            final URL url = classLoader.getResource(fileName);

            if (url != null) {
                final OServerCommandGetStaticContent.OStaticContent content = new OServerCommandGetStaticContent.OStaticContent();
                content.is = new BufferedInputStream(classLoader.getResourceAsStream(fileName));
                content.contentSize = -1;
                content.type = OServerCommandGetStaticContent.getContentType(url.getFile());
                return content;
            }
            return null;
        };
        final OServerNetworkListener httpListener = getOServer().getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
        if (httpListener != null) {
            final OServerCommandGetStaticContent command = (OServerCommandGetStaticContent) httpListener
                    .getCommand(OServerCommandGetStaticContent.class);
            command.registerVirtualFolder("studio", oCallable);
        }
    }

    @Override
    public void close() throws IOException {
        getOServer().shutdown();
    }

    public OServer getOServer() {
        return oServer;
    }

    public void activate() throws IOException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        System.setProperty("ORIENTDB_HOME", getHome());
        String dbPath = new File(getHome(), "databases").getAbsolutePath();
        OServerConfiguration  configuration = createDefaultServerConfiguration(dbPath);
        getOServer().startup(configuration);
        getOServer().activate();
        registerWwwAsStudio();
    }

    public String getHome() {
        return home;
    }
}
