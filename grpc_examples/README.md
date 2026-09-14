# gRPC examples from the Java and Python slides
This project includes examples for gRPC for Java and Python.
It includes unary lookup, server streaming, error handling, deadlines, and
generation/build instructions for both languages.

## Files and corresponding slides

| File | Purpose | 
| --- | --- |
| `catalog.proto` | Shared messages and service, including `ListCourses` | 
| `python/server.py` | Python unary and streaming handlers and server startup | 
| `python/client.py` | Python unary client | 
| `python/streaming_client.py` | Complete Python streaming client | 
| `python/generate.py` | Generate Python Protobuf and gRPC bindings | 
| `python/requirements.txt` | Pinned Python dependencies | 
| `java/src/main/java/Catalog.java` | Java unary handler and equivalent streaming handler | 
| `java/src/main/java/CatalogServer.java` | Java server startup | 
| `java/src/main/java/CatalogClient.java` | Java unary client | 
| `java/src/main/java/CatalogStreamingClient.java` | Complete Java streaming client |
| `java/pom.xml` | Java dependencies, generation, compilation, and execution | 
| `schema_examples/catalog_unary.proto` | Original unary-only contract | 
| `schema_examples/catalog_with_optional.proto` | Optional instructor and repeated topics | 
| `schema_examples/catalog_with_reserved.proto` | Instructor removed and its number/name reserved | 
| `schema_examples/rpc_patterns.proto` | Complete schema illustrating all four RPC signatures | 



The `schema_examples` files are independent teaching alternatives. They are excluded from the main build. Do not compile the three catalog alternatives together: they deliberately define the same messages and service. To experiment, copy one alternative to `catalog.proto` in a separate exercise project, then generate that project's bindings. The alternatives omit the streaming method.

## Prerequisites

- Python 3.10-3.13 with `pip` for the Python examples.
- JDK 17 or newer and Maven 3.8 or newer for the Java examples.
- Internet access to PyPI and Maven Central when installing dependencies.

The dependency versions are pinned to make the exercise repeatable. They are
example versions, not a claim that they are the latest releases. Java and Python
use different Protobuf runtime version numbers; their common `.proto` schema
defines the interoperable wire format.

## Python setup and generation

From the extracted `grpc_examples` directory:

```bash
cd python
python -m venv .venv
```

Activate the environment on macOS/Linux:

```bash
source .venv/bin/activate
```

Or use Windows Command Prompt:

```bat
.venv\Scripts\activate.bat
```

Then install dependencies and generate the bindings:

use python or python3

```bash
python3 -m pip install -r requirements.txt
python3 generate.py
```

Generation writes these files beside the Python scripts:

- `catalog_pb2.py`: Protobuf message classes and descriptors.
- `catalog_pb2.pyi`: Protobuf type information.
- `catalog_pb2_grpc.py`: client stub, servicer base, and registration helper.

The equivalent command, run from the `python` directory, is:

```bash
python3 -m grpc_tools.protoc -I.. --python_out=. --pyi_out=. --grpc_python_out=. ../catalog.proto
```

Start the server in terminal 1:

```bash
python3 server.py
```

In terminal 2, activate the same environment and run from the `python` directory:

```bash
python3 client.py
python3 client.py CS999
python3 streaming_client.py
```

Expected output, respectively:

```text
Intro to Computing 3
NOT_FOUND Unknown course
Computing
Networks
```

The unknown-course Python client exits with status 1, intentionally.

## Java setup and generation

From the extracted project directory:

```bash
cd java
mvn compile
```

Maven downloads the Protobuf compiler, the gRPC Java generator, and runtime
dependencies. It generates the bindings from the root `catalog.proto` and
compiles them together with the four Java application files. You do not need to
install `protoc` separately for this Maven build.

Generated Protobuf source files appear under
`target/generated-sources/protobuf/java/edu/demo/catalog/`:

- `CourseQuery.java` and `CourseQueryOrBuilder.java`
- `CourseInfo.java` and `CourseInfoOrBuilder.java`
- `Catalog.java`, the Protobuf descriptor container

The gRPC generator writes `CourseServiceGrpc.java` under
`target/generated-sources/protobuf/grpc-java/edu/demo/catalog/`.

The generated `edu.utexas.cs.Catalog` and the handwritten `Catalog` service
class have different fully qualified names. 

Start the Java server in terminal 1, from `java`:

```bash
mvn -q exec:java -Dexec.mainClass=CatalogServer
```

In terminal 2, from the same `java` directory:

```bash
mvn -q exec:java -Dexec.mainClass=CatalogClient
mvn -q exec:java -Dexec.mainClass=CatalogClient -Dexec.args=CS999
mvn -q exec:java -Dexec.mainClass=CatalogStreamingClient
```

The successful unary call prints `Intro to Computing 3`. The invalid ID prints a
gRPC `NOT_FOUND` status. The streaming client prints `Computing` and `Networks`.

For generation without compilation:

```bash
mvn generate-sources
```

## Java/Python interoperability

Both servers expose the same service at `127.0.0.1:50051`. Start only one server
at a time, then run either language's clients:

| Server | Compatible clients |
| --- | --- |
| Python `server.py` | Python unary/streaming and Java unary/streaming |
| Java `CatalogServer` | Python unary/streaming and Java unary/streaming |

Generate bindings for each client language before using it. Stop the running
server with Ctrl+C before switching server languages. These are localhost
plaintext teaching examples; TLS setup is discussed on slide 17.

## What is included and what the build generates

The archive contains the `.proto` definitions and all handwritten Java/Python
example files. Compiler-generated bindings are created by `python generate.py`
and Maven; they are not pre-generated in this archive.

Python source syntax and Maven XML structure were checked during preparation.
RPC execution and the Java compilation could not be checked here because this
environment has no Java compiler/Maven/gRPC toolchain and blocks dependency
downloads. The expected outputs above describe the examples, not a captured test
run.

## Official references

- [gRPC main website](https://grpc.io/)
- [Python quick start and generation](https://grpc.io/docs/languages/python/quickstart/)
- [Python server and client tutorial](https://grpc.io/docs/languages/python/basics/)
- [Java server and client tutorial](https://grpc.io/docs/languages/java/basics/)
- [gRPC Java dependencies and build setup](https://github.com/grpc/grpc-java)
- [Protobuf proto3 language guide](https://protobuf.dev/programming-guides/proto3/)
- [Maven Protobuf plugin](https://www.xolstice.org/protobuf-maven-plugin/compile-custom-mojo.html)
