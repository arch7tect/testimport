package ru.neoflex.meta.testimport;

import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.config.*;
import com.orientechnologies.orient.server.network.OServerNetworkListener;
import com.orientechnologies.orient.server.network.protocol.http.ONetworkProtocolHttpAbstract;
import com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent;

import java.io.*;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Main implements Closeable {
    public static final String TESTIMPORT = "testimport";
    private File home = new File(System.getProperty("user.dir"), ".orientdb");
    private OServer oServer;
    private OServerConfiguration configuration;

    public Main() throws Exception {
        deleteDirectory(home);
        System.setProperty("ORIENTDB_HOME", home.getAbsolutePath());
        String dbPath = new File(home, "databases").getAbsolutePath();
        this.oServer = OServerMain.create(false);
        this.configuration = createDefaultServerConfiguration(dbPath);
        oServer.startup(configuration);
        oServer.activate();
        registerWwwAsStudio();
    }

    public OServerConfiguration createDefaultServerConfiguration(String dbPath) {
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
                commands = new OServerCommandConfiguration[] {new OServerCommandConfiguration() {{
                    implementation = "com.orientechnologies.orient.server.network.protocol.http.command.get.OServerCommandGetStaticContent";
                    pattern = "GET|www GET|studio/ GET| GET|*.htm GET|*.html GET|*.xml GET|*.jpeg GET|*.jpg GET|*.png GET|*.gif GET|*.js GET|*.css GET|*.swf GET|*.ico GET|*.txt GET|*.otf GET|*.pjs GET|*.svg";
                    parameters = new OServerEntryConfiguration[] {
                            new OServerEntryConfiguration("http.cache:*.htm *.html", "Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache"),
                            new OServerEntryConfiguration("http.cache:default", "Cache-Control: max-age=120"),
                    };
                }}};
                parameters = new OServerParameterConfiguration[] {
                        new OServerParameterConfiguration("network.http.charset","UTF-8"),
                        new OServerParameterConfiguration("network.http.jsonResponseError","true")
                };
            }});
        }};
        configuration.users = new OServerUserConfiguration[] {
                new OServerUserConfiguration("root", "ne0f1ex", "*"),
                new OServerUserConfiguration("admin", "admin", "*"),
        };
        configuration.properties = new OServerEntryConfiguration[] {
                new OServerEntryConfiguration("server.cache.staticResources", "false"),
                new OServerEntryConfiguration("log.console.level", "info"),
                new OServerEntryConfiguration("log.console.level", "info"),
                new OServerEntryConfiguration("log.file.level", "fine"),
                new OServerEntryConfiguration("server.database.path", dbPath),
                new OServerEntryConfiguration("plugin.dynamic", "true"),
        };
        return configuration;
    }

    public void registerWwwAsStudio() {
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
        final OServerNetworkListener httpListener = oServer.getListenerByProtocol(ONetworkProtocolHttpAbstract.class);
        if (httpListener != null) {
            final OServerCommandGetStaticContent command = (OServerCommandGetStaticContent) httpListener
                    .getCommand(OServerCommandGetStaticContent.class);
            command.registerVirtualFolder("studio", oCallable);
        }
    }


    public static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public void run() throws IOException {
        if (!oServer.existsDatabase(TESTIMPORT)) {
            oServer.createDatabase(TESTIMPORT, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());
        }
        try (ODatabaseDocumentInternal db = oServer.openDatabase(TESTIMPORT)) {
            OClass userClass = db.createVertexClass("User");
            userClass.createProperty("name", OType.STRING);
            OClass groupClass = db.createVertexClass("Group");
            groupClass.createProperty("name", OType.STRING);
            OClass memberOfClass = db.createEdgeClass("MemberOf");
            memberOfClass.createProperty("since", OType.DATE);
            db.begin(OTransaction.TXTYPE.OPTIMISTIC);
            try {
                List<OVertex> groups = new ArrayList<>();
                for (int i = 0; i < 10; ++i) {
                    OVertex oVertex = db.newVertex(groupClass);
                    oVertex.setProperty("name", "group"+i);
                    oVertex.save();
                    groups.add(oVertex);
                }
                List<OVertex> users = new ArrayList<>();
                for (int i = 0; i < 100; ++i) {
                    OVertex oVertex = db.newVertex(userClass);
                    oVertex.setProperty("name", "user"+i);
                    oVertex.save();
                    users.add(oVertex);
                }
                Random rand = new Random();
                for (int i = 0; i < 1000; ++i) {
                    OVertex user = users.get(rand.nextInt(users.size()));
                    OVertex group = groups.get(rand.nextInt(groups.size()));
                    OEdge oEdge = db.newEdge(user, group, memberOfClass);
                    oEdge.setProperty("since", new Date());
                    oEdge.save();
                }
            }
            finally {
                db.commit(true);
            }
            File dir = new File(home, "export");
            dir.mkdirs();
            File file = new File(dir, TESTIMPORT + "_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".json.gz");
            try (OutputStream os = new FileOutputStream(file)) {
                try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(os)) {
                    ODatabaseExport export = new ODatabaseExport(db, gzipOutputStream, System.out::print);
                    try {
                        export.run();
                    }
                    finally {
                        export.close();
                    }
                }
            }
            try (InputStream is = new FileInputStream(file)) {
                try(GZIPInputStream gzipInputStream = new GZIPInputStream(is)) {
                    ODatabaseImport import_ = new ODatabaseImport(db, gzipInputStream, System.out::print);
                    try {
                        import_.setMerge(false);
                        import_.run();
                    }
                    finally {
                        import_.close();
                    }
                }
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("Hello from orientdb!");
        try {
            try (Main main = new Main()) {
                main.run();
                main.oServer.waitForShutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        oServer.shutdown();
    }
}
