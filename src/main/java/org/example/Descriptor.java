package org.example;

import org.apache.avro.Schema;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.table.HoodieJavaTable;

import java.util.List;

public class Descriptor {

  private final HoodieJavaTable table;
  public Descriptor(HoodieJavaTable table) {
    this.table = table;
  }

  public void toSchema() throws Exception {
    TableSchemaResolver resolver = new TableSchemaResolver(table.getMetaClient());
    Schema schema = resolver.getTableAvroSchema(false);
    List<Schema.Field> fields = schema.getFields();

    fields.forEach(f -> {
      System.out.println(f.name());
      System.out.println(f.schema().getLogicalType());
      System.out.println(f.doc());
      System.out.println(f.schema());
      System.out.println(f.order());

    });

  }

}
