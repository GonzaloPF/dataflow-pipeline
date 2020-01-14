package main;



import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;



import java.io.IOException;

/*
compile exec:java -Dexec.mainClass=main.pipelineBQ 
-e -Dexec.args=
"--project=<project>
 --numWorkers=50
 --stagingLocation=gs://<bucket>/test_xml 
 --tempLocation=gs://<bucket>/test_xml 
 --runner=DataflowRunner
 --region=us-central1"
 */



public class fromListFilestoBQ {
	
	final static String COL_PRE = "col";
	final static int NUM_COLUMNS = 1;
	final static String END_LINE = "\n";
	final static String TABLE = "pruebas.tabla_pruebas_csv2";
	
	public static void main(String[] args) {
		
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
		
		Pipeline p = Pipeline.create(options);
		
		
		//Sequence generator
		PCollection<FileIO.ReadableFile> files = p.apply("createListFiles", FileIO.match()
				.withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)
				.filepattern("gs://<bucket>/multiple_csv/*.csv"))
				.apply(FileIO.readMatches());
		PCollection<TableRow> entitiesRow = files.apply("CreateRows", ParDo.of(new createList()));
		
		
		
		//creating a new BQ schem, in this case we will have only one column
		String tableSchemaJson = schema(NUM_COLUMNS);
		//write the content in the table, since is batch the table won't be created until all the data 
		// is processed
		entitiesRow.apply("toBQ", BigQueryIO.writeTableRows()
				.to(TABLE)
				.withJsonSchema(tableSchemaJson)
				.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
		
		
		
		p.run();
		System.exit(0);
	}
	public static String schema(int columns) {
		//will generate an schema with the number of the columns set
		String strColumns = "";
		for (int i=0; i<columns; i++) {
			String column = "       {"
					+ "           \"name\": \"col" + i + "\","
					+ "           \"type\": \"STRING\","
					+ "           \"mode\": \"REQUIRED\""
					+ "        },";
			strColumns += column; 
		}
		
		String tableSchemaJson = 
				""
				        + "{"
						+ " \"fields\": ["
						+ strColumns.substring(0, strColumns.length()-1)
						+ "   ]"
						+ "}";
		return tableSchemaJson;
	}

	public static class createList extends DoFn<FileIO.ReadableFile, TableRow>{
		//this class transform each file in a list of TableRow for each row in this CSV
		
		private static final long serialVersionUID = 1L;
		@ProcessElement
		public void createList(@Element FileIO.ReadableFile word, OutputReceiver<TableRow> out) {
			String column [] = null;
			try {
				//transform the csv file in a CSV
				byte[] bytes = word.readFullyAsBytes();
				String str_column = new String(bytes);
				//split the string by rows
				column = str_column.split(END_LINE);
				for (int lin=0; lin<column.length; lin++) {
					TableRow tab = new TableRow();
					tab.set(COL_PRE+"0", column[lin]);
					//for each row we return a new TableRow to be read by BigQuery
					out.output(tab);
				}
				
				
			} catch (IOException e) {
				System.out.println("Some exception");
			}
			
			
		}
	}
	
	
	
	
}