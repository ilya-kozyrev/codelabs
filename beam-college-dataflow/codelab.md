author: Ilya Kozyrev
summary: In this codelab you'll learn how to add new input source to the Dataflow template using Beam Schema API. And how to build Beam pipeline into Dataflow Template and run it in GCP
id: data-flow-templates-new-source-schema-api
categories: Dataflow,Beam,GCP,Dataflow Template
environments: Web
status: Draft
feedback link:https://github.com/akvelon/DataflowTemplates/tree/BeamRowDemo/v2/protegrity-data-tokenization
analytics account: 

# Add new input source to the Dataflow Template using Beam Schema API

## Introduction
Duration: 0:05:00

#### Goals

In this codelab you'll add new input source to existing pipeline, build pipeline to the Dataflow template and run it on GCP.
Through this codelab, you'll understand how to work with Beam Schema API and how this API help you to support multiple formats in your pipeline.

#### What you'll build

* New transformation for .parquet format into the Beam Row abstraction
* New transformation from Beam Row into .parquet format
* Read and Write methods for Beam using those transformations

#### What you'll learn
* How to use Beam Schema Api
* How to work with ParquetIO
* How to build pipeline into Dataflow Template
* How to run Dataflow Template in GCP

#### What you'll need
* A computer with a modern web browser installed (Chrome is recommended)
* Text editor
* A Google account
* Git installed to your computer


## Environment Setup
Duration: 0:15:00

#### Install Java Environment

In order to work with Beam Java SDK you need *Java 1.8*, *mvn* (the java command line tool for build) installed.

1) Download and install the [Java Development Kit (JDK)](https://www.oracle.com/technetwork/java/javase/downloads/index.html) version 8. Verify that the [JAVA_HOME](https://docs.oracle.com/javase/8/docs/technotes/guides/troubleshoot/envvars001.html) environment variable is set and points to your JDK installation.

2) Download and install [Apache Maven](https://maven.apache.org/download.cgi) by following Maven’s [installation guide](https://maven.apache.org/install.html) for your specific operating system.



#### Install Git
The instructions below are what worked for me on Mac, but you can also find instructions [here](https://git-scm.com/download/)

```bash
$ brew install git
```

#### Clone the template repository
```bash
$ git clone https://github.com/akvelon/DataflowTemplates.git
```

#### Switch to the working branch
```bash
$ git checkout ProtegrityIntegrationTemplate
```

## Project overview
Duration: 0:05:00

In your cloned  repository folder you might see `src` and `v2` folders. There are classic 
templates contains in `src` folder and Flex templates contains in `v2` folder.

In this codelab you will work with flex templates. 

Go to the template folder DataflowTemplates/v2/protegrity-data-tokenization

In the target folder you might see the template packages. Let's go through each of them.

![image_caption](https://i.ibb.co/TRpGHY5/Screenshot-2021-03-23-at-16-39-21.png")
* `options` package contains all pipeline parameters and logic around them
* `templates` package contains main class of presented template. There is pipeline creation and applying all transformations
* `transforms` package contains all Beam transform that applies to the pipeline
    * `io` subpackage contains transforms for input/output, e.g. to convert from format to Row and vise versa.
  
* `utils` package contains utils that used in pipeline for supporting operations, such us schema parsing or csv building
* `resources` folder contains metadata file, that will be need when you will build pipeline to the template.

As a next step you will go throughout the main class.

## Main template class
Duration: 0:15:00

Let's go to the main template class `ProtegrityDataTokenization` and describe it. This class locates in `v2/protegrity-data-tokenization/src/main/java/com/google/cloud/teleport/v2/templates/ProtegrityDataTokenization.java`

As you now most java programs have main class with `main` method as an entry point for the application. There is very similar structure.

In `ProtegrityDataTokenization` class you may see main method with option parsing and calling run method
```java 
  public static void main(String[] args) {
    ProtegrityDataTokenizationOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(ProtegrityDataTokenizationOptions.class);
    FileSystems.setDefaultPipelineOptions(options);

    DataflowPipelineOptions dataflowOptions = PipelineOptionsFactory.fromArgs(args)
        .withoutStrictParsing()
        .withValidation()
        .as(DataflowPipelineOptions.class);

    run(options, dataflowOptions);
  }
```
And in `run` method presented pipeline implements basic beam pipeline implementation flow. 
1) Pipeline creation:
```java 
Pipeline pipeline = Pipeline.create(options);
```
2) Applying necessary transformations:
```java
pipeline.apply()
```
3) Pipeline running by calling `run` method.
```java
pipeline.run();
```

Let's take a look at two interesting things in this class.

#### Schema
In the presented pipeline you may see schema reading, it's implementation to read schema in JSON BigQuery compatible format format to build Beam Rows
```java 
schema = new SchemasUtils(options.getDataSchemaGcsPath(), StandardCharsets.UTF_8);
```

#### Coder
As you know in Beam provides coder mechanism to serialisation/deserialization process. 
And for operate with some types you need specify coders by ```CoderRegistry``` or by 
applying ```.setCoder()``` to the PCollection. And for this case used Beam Schema API to represent 
data as Rows in PCollection.
```java
CoderRegistry coderRegistry = pipeline.getCoderRegistry();
coderRegistry
    .registerCoderForType(RowCoder.of(schema.getBeamSchema()).getEncodedTypeDescriptor(), 
        RowCoder.of(schema.getBeamSchema()));
```

Next step: You will take a look at schema in BigQuery compatible format.

## Schema from Json
Duration: 0:15:00

Beam schema requires a type mapping mechanism from the input format. 
The type matching mechanism depends on the input format. There is might be structured formats like 
BigQuery datasets with metadata of tables where you can automatically take schema. 
And structured formats like JSON and CSV where you need to specify the schema for converting into 
Beam Rows. 

And one of the best approaches for the second case is JSON in BigQuery Schema compatible format.

```json
{
  "fields": [
    {
      "name": "ssn",
      "type": "STRING",
      "mode": "REQUIRED"
    },
    {
      "name": "firstname",
      "type": "STRING",
      "mode": "REQUIRED"
    },
    {
      "name": "lastname",
      "type": "STRING",
      "mode": "NULLABLE"
    }
  ]
}
```
You need to create json with `fields` collection that includes all input fields as json objects.

You need to create JSON with a `fields` collection that includes all input fields as JSON objects.

Each object should contains three attributes
* `name` Name of the field
* `type` Type of the field, in BigQuery [types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types?hl=pl). Likely it's very closely with Beam schema Type. All Beam types supports with BigQuery
* `mode` Might be `REQUIRED` or `NULLABLE`

### Schema parsing

To parse schema you need to implement parsing from JSON string using `BigQueryHelpers.fromJsonString()`

```java
void parseJson(String jsonSchema) throws UnsupportedOperationException {
    TableSchema schema = BigQueryHelpers.fromJsonString(jsonSchema, TableSchema.class);
    validateSchemaTypes(schema);
    bigQuerySchema = schema;
    jsonBeamSchema = BigQueryHelpers.toJsonString(schema.getFields());
}
```

And convert ```TableSchema``` to Beam `Schema` using `org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.fromTableSchema`

You may see how it implements in presented pipeline `v2/protegrity-data-tokenization/src/main/java/com/google/cloud/teleport/v2/utils/SchemasUtils.java` class

Nex step: IO transform overview

## GcsIO overview
Duration: 0:10:00

To transform many input formats to the Beam Row and from Beam Row into destination format, presented
pipeline implements IO transforms. Let's focus on GcsIO that works with Google Cloud Storage.

In the `v2/protegrity-data-tokenization/src/main/java/com/google/cloud/teleport/v2/transforms/io/GcsIO.java` you may see `FORMAT` enum, that covers all supported formats.
```java
public enum FORMAT {
    JSON,
    CSV,
    AVRO
}
```

For IO functionality `GcsIO` class provides two methods `read` 
```java
public PCollection<Row> read(Pipeline pipeline, SchemasUtils schema) {
    switch (options.getInputGcsFileFormat()) {
      case CSV:
        ...
      case JSON:
        return readJson(pipeline)
            .apply(new JsonToBeamRow(options.getNonTokenizedDeadLetterGcsPath(), schema));
      case AVRO:
        return readAvro(pipeline, schema.getBeamSchema());
      default:
        throw new IllegalStateException(
            "No valid format for input data is provided. Please, choose JSON or CSV.");

    }
  }
```

and `write`
```java
public POutput write(PCollection<Row> input, Schema schema) {
    switch (options.getOutputGcsFileFormat()) {
      case JSON:
        return writeJson(input);
      case AVRO:
        return writeAvro(input, schema);
      case CSV:
        return writeCsv(input, schema.getFieldNames());
      default:
        throw new IllegalStateException(
            "No valid format for output data is provided. Please, choose JSON or CSV.");

    }
}
```

For each supported format GcsIO encapsulates transformations from source format to Beam Row and 
from Beam Row to destination format for writing results. e.g. `writeJson` or `readJson`. 
That takes pipeline and Schema as argument.

Next step: You will implement parquet files reading and writing and transformation this into the Beam Row.

## Parquet IO Read
Duration: 0:15:00

To implement new parquet source read and write, firstly you need to add new value into `FORMAT` enum 

```java
public enum FORMAT {
    JSON,
    CSV,
    AVRO,
    PARQUET
  }
```

Next you need to create method for read parquet from filesystem and convert it into Beam Row.

To read parquet you might use ParquetIO from Beam SDK. You need to apply `ParquetIO.read()` to the pipeline object.
```java
PCollection<GenericRecord> parquetRecords =
        pipeline.apply(ParquetIO.read(avroSchema).from(options.getInputGcsFilePattern()));
```

`ParquetIO.read()` takes Avro schema as a argument. And you need to convert Beam Schema to the Avro
schema using `AvroUtils.toAvroSchema()`
```
org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);
```

Applying `ParquetIO.read()` to the pipeline produce `PCollection` of `GenericRecord` and you should 
convert it into the Beam Row. Easiest way to do that is applying `MapElements.into` to the 
`PCollection<GenericRecord>`. One thing that you need to catch - transform function from 
`GenericRecord` into `Row`. And likely it also contains in `AvroUtils`.

```java
parquetRecords
    .apply("GenericRecordsToRow", MapElements.into(TypeDescriptor.of(Row.class))
        .via(AvroUtils.getGenericRecordToRowFunction(beamSchema)))
```
The last thing that you need to do - specifying the coder for the new PCollection. 
A fully implemented method: 


```
private PCollection<Row> readParquet(Pipeline pipeline, Schema beamSchema) {
    org.apache.avro.Schema avroSchema = AvroUtils.toAvroSchema(beamSchema);

    PCollection<GenericRecord> parquetRecords =
        pipeline.apply(ParquetIO.read(avroSchema).from(options.getInputGcsFilePattern()));

    return parquetRecords
        .apply("GenericRecordsToRow", MapElements.into(TypeDescriptor.of(Row.class))
            .via(AvroUtils.getGenericRecordToRowFunction(beamSchema)))
        .setCoder(RowCoder.of(beamSchema));
 }
```


## Parquet IO Write
Duration: 0:15:00



## Build the Template
Duration: 0:05:00



## Run the Template
Duration: 0:05:00



## Contribute to DataflowTemplates
Duration: 0:05:00


## Finish
Duration: 0:05:00