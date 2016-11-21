package com.dangdang.ddframe.rdb.sharding.config.yaml.hash;

import org.apache.commons.dbcp.BasicDataSource;
import org.h2.tools.RunScript;
import org.junit.BeforeClass;

import javax.sql.DataSource;
import java.io.File;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author DonneyYoung
 **/
public abstract class AbstractHashTest {

    @BeforeClass
    public static void createSchema() throws SQLException {
        for (String each : getSchemaFiles()) {
            RunScript.execute(createDataSource(getFileName(each)).getConnection(), new InputStreamReader(AbstractHashTest.class.getClassLoader().getResourceAsStream(each)));
        }
    }

    protected static DataSource createDataSource(final String dsName) {
        BasicDataSource result = new BasicDataSource();
        result.setDriverClassName(org.h2.Driver.class.getName());
        result.setUrl(String.format("jdbc:h2:mem:%s;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=false;MODE=MYSQL", dsName));
        result.setUsername("sa");
        result.setPassword("");
        return result;
    }

    private static String getFileName(final String dataSetFile) {
        String fileName = new File(dataSetFile).getName();
        if (-1 == fileName.lastIndexOf(".")) {
            return fileName;
        }
        return fileName.substring(0, fileName.lastIndexOf("."));
    }

    private static List<String> getSchemaFiles() {
        return Arrays.asList("schema/db0.sql", "schema/db1.sql", "schema/db2.sql", "schema/db3.sql");
    }
}
