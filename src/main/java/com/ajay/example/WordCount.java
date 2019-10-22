package com.ajay.example;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.DefaultValueFactory;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Aggregator;
import com.google.cloud.dataflow.sdk.transforms.Count;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class WordCount {

	static class ExtractWordFn extends DoFn<String, String> {

		private final Aggregator<Long, Long> emptyLines = createAggregator("aggregator", new Sum.SumLongFn());

		@Override
		public void processElement(DoFn<String, String>.ProcessContext context) throws Exception {
			if (context.element().isEmpty()) {
				emptyLines.addValue(1L);
			}

			String words[] = context.element().split("[^A-Za-z']");
			for (String word : words) {
				if (!word.isEmpty()) {
					context.output(word);
				}
			}
		}
	}

	public static class FormatAsTextFn extends DoFn<KV<String, Long>, String> {

		@Override
		public void processElement(DoFn<KV<String, Long>, String>.ProcessContext context) throws Exception {
			context.output(context.element().getKey() + ": " + context.element().getValue());
		}
	}

	public static class CountWords extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
		@Override
		public PCollection<KV<String, Long>> apply(PCollection<String> lines) {

			// Convert lines of text into individual words.
			PCollection<String> words = lines.apply(ParDo.of(new ExtractWordFn()));

			// Count the number of times each word occurs.
			PCollection<KV<String, Long>> wordCounts = words.apply(Count.<String>perElement());

			return wordCounts;
		}
	}

	public interface WordCountOptions extends PipelineOptions {
		@Description("Path of the file to read from")
		@Default.String("gs://data-fusion-files/india.txt")
		String getInputFile();

		void setInputFile(String value);

		@Description("Path of the file to write to")
		@Default.InstanceFactory(OutputFactory.class)
		String getOutput();

		void setOutput(String value);

		/**
		 * Returns "gs://${YOUR_STAGING_DIRECTORY}/counts.txt" as the default
		 * destination.
		 */
		class OutputFactory implements DefaultValueFactory<String> {
			@Override
			public String create(PipelineOptions options) {
				DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
				if (dataflowOptions.getStagingLocation() != null) {
					return GcsPath.fromUri(dataflowOptions.getStagingLocation()).resolve("counts.txt").toString();
				} else {
					throw new IllegalArgumentException("Must specify --output or --stagingLocation");
				}
			}
		}

	}

	public static void main(String[] args) {

		WordCountOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCountOptions.class);
		Pipeline p = Pipeline.create(options);
		p.apply(TextIO.Read.named("ReadLines").from(options.getInputFile()))
			.apply(new CountWords())
			.apply(ParDo.of(new FormatAsTextFn()))
			.apply(TextIO.Write.named("WriteCounts").to(options.getOutput()));
		p.run();
	}

}
