package io.unitycatalog.spark;

import static io.unitycatalog.spark.UCProxyTestFixture.CATALOG_NAME;
import static io.unitycatalog.spark.UCProxyTestFixture.NAMESPACE;
import static io.unitycatalog.spark.UCProxyTestFixture.SCHEMA_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.unitycatalog.client.model.ColumnInfo;
import io.unitycatalog.client.model.ColumnTypeName;
import io.unitycatalog.client.model.DataSourceFormat;
import io.unitycatalog.client.model.ListTablesResponse;
import io.unitycatalog.client.model.TableInfo;
import io.unitycatalog.client.model.TableType;
import io.unitycatalog.hadoop.UCCredentialHadoopConfs.TableOperation;
import io.unitycatalog.hadoop.internal.CredPropsUtil;
import io.unitycatalog.hadoop.internal.auth.AwsCredential;
import io.unitycatalog.hadoop.internal.auth.GenericCredentialFetcher;
import io.unitycatalog.hadoop.internal.id.TableCredId;
import io.unitycatalog.spark.utils.OptionsUtil;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.V1Table;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for the {@code warehouse} catalog option: the UC catalog name used for API calls. */
public class CatalogOptionsSuite {

  private static final String LOCATION = "s3://test-bucket/tables/t";
  private static final String UC_CATALOG = "uc_main";

  private final AtomicInteger readWriteAttempts = new AtomicInteger();
  private final AtomicInteger readAttempts = new AtomicInteger();

  @BeforeEach
  public void setUp() {
    readWriteAttempts.set(0);
    readAttempts.set(0);
    CredPropsUtil.genericCredFetcherFactory =
        (apiClient, credId) ->
            () -> {
              String operation = ((TableCredId) credId).tableOperation();
              if (TableOperation.READ_WRITE.value().equals(operation)) {
                readWriteAttempts.incrementAndGet();
              } else {
                readAttempts.incrementAndGet();
              }
              return List.of(
                  new AwsCredential("access-key", "secret-key", "session-token", null, LOCATION));
            };
  }

  @AfterEach
  public void reset() {
    CredPropsUtil.genericCredFetcherFactory = GenericCredentialFetcher::create;
  }

  @Test
  public void warehouseAddressesUcCatalogWhileSparkKeepsItsOwnName() throws Exception {
    UCProxyTestFixture fixture =
        new UCProxyTestFixture().build(Map.of(OptionsUtil.WAREHOUSE, UC_CATALOG));
    TableInfo ucTable = tableInfo(UC_CATALOG, "t_alias", DataSourceFormat.PARQUET);
    when(fixture.mockTablesApi.getTable(
            eq(UC_CATALOG + "." + SCHEMA_NAME + ".t_alias"), eq(true), eq(true)))
        .thenReturn(ucTable);
    when(fixture.mockTablesApi.listTables(eq(UC_CATALOG), eq(SCHEMA_NAME), anyInt(), any()))
        .thenReturn(new ListTablesResponse().tables(List.of(ucTable)));

    Table table = fixture.proxy.loadTable(Identifier.of(NAMESPACE, "t_alias"));
    assertThat(((V1Table) table).v1Table().identifier().catalog().get()).isEqualTo(CATALOG_NAME);

    assertThat(fixture.proxy.listTables(NAMESPACE))
        .containsExactly(Identifier.of(NAMESPACE, "t_alias"));
    verify(fixture.mockTablesApi).listTables(eq(UC_CATALOG), eq(SCHEMA_NAME), anyInt(), any());
  }

  @Test
  public void withoutWarehouseTheSparkCatalogNameIsTheUcCatalog() throws Exception {
    UCProxyTestFixture fixture = new UCProxyTestFixture().build();
    when(fixture.mockTablesApi.getTable(
            eq(CATALOG_NAME + "." + SCHEMA_NAME + ".t_plain"), eq(true), eq(true)))
        .thenReturn(tableInfo(CATALOG_NAME, "t_plain", DataSourceFormat.PARQUET));

    fixture.proxy.loadTable(Identifier.of(NAMESPACE, "t_plain"));
    assertThat(readWriteAttempts.get()).isEqualTo(1);
    assertThat(readAttempts.get()).isEqualTo(0);
  }

  /** Table ids are unique per name: the hadoop credential cache is JVM-global. */
  private static TableInfo tableInfo(String catalog, String name, DataSourceFormat format) {
    return new TableInfo()
        .catalogName(catalog)
        .schemaName(SCHEMA_NAME)
        .name(name)
        .tableId("table-id-" + name)
        .tableType(TableType.EXTERNAL)
        .storageLocation(LOCATION)
        .dataSourceFormat(format)
        .columns(
            List.of(
                new ColumnInfo()
                    .name("id")
                    .typeName(ColumnTypeName.INT)
                    .typeText("int")
                    .typeJson(
                        "{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}")
                    .nullable(true)
                    .position(0)));
  }
}
