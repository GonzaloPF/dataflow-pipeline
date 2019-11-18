package main;



import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

import com.google.api.services.bigquery.model.TableRow;
import java.util.Random;

/*
compile exec:java -Dexec.mainClass=main.pipelineBQ 
-e -Dexec.args=
"--project=$PROJECT 
 --numWorkers=20
 --stagingLocation=$BUCKET 
 --tempLocation=$BUCKET 
 --runner=DataflowRunner 
 --streaming=true
 --region=us-central1 
 --experiments=enable_streaming_engine"
 */



public class pipelineBQ {
	
	final static String COL_PRE = "col";
	final static int NUM_COLUMNS = 100;
	//In batch case we set the number of rows
	//final static int NUM_ROWS = 100;
	final static String TABLE = "$DATASET.$TABLE_NAME";
	
	public static void main(String[] args) {
		
		DataflowPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(DataflowPipelineOptions.class);
		
		Pipeline p = Pipeline.create(options);
		
		
		//Sequence generator
		PCollection<Long> datos = p.apply("createFlow", GenerateSequence.from(0));
		//In batch case we set a limit
		//PCollection<Long> datos = p.apply("createFlow", GenerateSequence.from(0).to(numRows));
		
		//Initialize the row with the number of the row
		PCollection<String[]> datos_row = datos.apply("CreateStructure", new createRows(NUM_COLUMNS));
		//Fill each column with the generated word
		PCollection<String[]> datos_str = datos_row.apply("CreateData", ParDo.of(new wordFN()));
		
		
		//Set TableRow shape in order to be read by Bigquery sink
		PCollection<TableRow> entitiesTuple = datos_str.apply("CreateRow", ParDo.of(new createRow()));
		//Send the data to BQ sink with the appropriate params
		String tableSchemaJson = schema(NUM_COLUMNS);
		entitiesTuple.apply("toBQ", BigQueryIO.writeTableRows()
				.to(TABLE)
				.withJsonSchema(tableSchemaJson)
				.withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED));
		//run the pipeline
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
	public static class wordFN extends DoFn<String[], String[]>{
		//this class will generate new random words and will fill the array
		
		private static final long serialVersionUID = 1L;
		final static String letters [] = new String[] {"a","b","c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "ñ", "o",
				"p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", " "};
		@ProcessElement
		public void wordFN(@Element String[] arr, OutputReceiver<String[]> out) {
			for (int i=0; i<arr.length; i++) {
				arr[i] = arr[i] + "-" + word();
			}
			
			
			out.output(arr);
		}
		public static String word() {
			
			Random rand = new Random();
			String line = "";
			int posin = rand.nextInt(50)+1;
			for (int i=0; i<posin; i++) {
				int pos = rand.nextInt(26);
				line = line + letters[pos];
			}
			return line;
		}
		
	}
	public static class createRow extends DoFn<String[], TableRow>{
		//this class transform the array filled into TableRow format
		private static final long serialVersionUID = 1L;
		@ProcessElement
		public void createRow(@Element String word[], OutputReceiver<TableRow> out) {
			TableRow tab = new TableRow();
			for (int i=0; i<word.length; i++) {
				tab.set(COL_PRE+i, word[i]);
			}
			
			
			out.output(tab);
		}
	}
	public static class createRows extends PTransform<PCollection<Long>, PCollection<String[]>>{
		//this class transform each generated long into a new array filled with the number of the row
		private static final long serialVersionUID = 1L;
		private final int numColumns;
		
		public createRows(int numColumns) {
			this.numColumns = numColumns;
		}

		@Override
		public PCollection<String[]> expand(PCollection<Long> input) {
			return input.apply("CreateData", ParDo.of(new DoFn<Long, String[]>(){
				
				private static final long serialVersionUID = 1L;

				@ProcessElement
				public void processElement(@Element Long lon, OutputReceiver<String[]> out) throws Exception {
					String [] exit = new String [numColumns];
					for (int i=0; i<exit.length; i++) {
						exit[i] = String.valueOf(lon);
					}
					out.output(exit);
				}
			}));
				
		}
	}
	
	
	
}