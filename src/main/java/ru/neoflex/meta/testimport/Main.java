package ru.neoflex.meta.testimport;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.tool.ODatabaseExport;
import com.orientechnologies.orient.core.db.tool.ODatabaseImport;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.tx.OTransaction;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Main extends LocalOrientDBServer {
    public static final String TESTIMPORT = "testimport";

    public Main() throws Exception {
        super(new File(System.getProperty("user.dir"), ".orientdb").getAbsolutePath());
        deleteDirectory(new File(getHome()));
        activate();
    }


    public static void deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        directoryToBeDeleted.delete();
    }

    public void run() throws IOException {
        if (!getOServer().existsDatabase(TESTIMPORT)) {
            getOServer().createDatabase(TESTIMPORT, ODatabaseType.PLOCAL, OrientDBConfig.defaultConfig());
        }
        try (ODatabaseDocumentInternal db = getOServer().openDatabase(TESTIMPORT)) {
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
            File dir = new File(getHome(), "export");
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
        try {
            try (Main main = new Main()) {
                main.run();
                main.getOServer().waitForShutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
