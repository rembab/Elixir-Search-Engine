# Search Engine
## What is the project?
It's a highly optimized search engine running on a **ErlangVM** via **Elixir**. It uses **livebook** for basic UI.

The currently implemented features include:
- parsing the input documents from a user specified **NJSON** format files,
- document content is **cleaned** and **stemmed**, 
- storing those documents and a dictionary of words inside a local **sqlite3** database,
- the dictionary is filtered using **TF-IDF** score,
- calculating the **BM25** vector of each document in the database and storing it as an **Erlang** tuple structure **BLOB** for high performance,
- constructing the **full matrix** table inside the **sqlite3** database for efficient retrieval,
- querying the search engine and getting results based on **BM25** distance.
- noise reduction via **low rank approximation** using **sklearn.decomposition.TruncatedSVD**,
- caching already computed matrices speeding up further searches,

The entiriety of the above functionalities have been implemented with performance and memory efficiency in mind. 

Disclaimer: the program still needs a lot of memory and a decent CPU to run fast, but that is just a specification of a search engine. Unless considering datasets with millions of records and a machine of less than **16GB** RAM a crash should be an anomaly.
## Performance
The engine has not been extensively tested, although I have performed basic performance test on my machine.

**Test environment specs:** 16GB RAM, 12th Gen i5-12450H CPU

**Chosen dataset:** www.kaggle.com/datasets/Cornell-University/arxiv - metadata of scholarly arXiv articles. I've vectorized the **title** and **abstract** of each article

**Number of documents in the database:** 1 million

**Number of terms in the dictionary:** 500k

**Time spent constructing the document and term database:** 6 minutes 42 seconds

**Time spent calculating vectors and building full matrix:** 7 minutes 22 seconds

**Time spent performing SVD and caching a matrix of k = 300:**  6 minutes 58 seconds

For a dataset of this size, im very much satisfied. This environment could easily handle bigger datasets with enough **RAM**. Biggest bottleneck is loading the matrix rows for multiplication and svd inside the Python script.

## Why Elixir?
Elixir might seem like an odd choice for this kind of project at first. Optimizing a search engine requires a lot of heavy math, especially matrix manipulation. Elixir is definitely not built for numerical analysis and an equivalent of this implementation in **Rust**, **C/C++** or well written **Python**  would definitely be faster. Nevertheless, I am still satisfied with the performance I've been able to archieve. **Elixir** choice can also even be justified by the following points:
1. Highly optimized **Streams** and **Flows** allow for efficient memory manipulation and don't clog the garbage collector - a crutial functionality for dealing with large matrices,
2. **Flow** allows for seamless multithreading making the engine ready to be run on high end machines  with the biggest bottleneck being their specs,
3. The heaviest matrix operations can still be performed by a **Python** script, which the ErlangVM can seamlessly and asynchronically communicate with,
3. Theoretically, this engine is a great backbone to function on a server and handle queries from different connected devices seamlessly thanks to the Erlang VM,
4. I wanted to learn Elixir, so I used this project to do so,
5. Coding in Elixir is fun :).

## How to run?
### Method 1: docker image
Simply run  the project's [docker image](https://hub.docker.com/repository/docker/rembab/search_engine/general) and open http://localhost:8080/apps in your browser. This will run the search engine as a livebook application in your browser. 

To run the docker image simply open your system's command line (terminal/cmd/powershell etc.) and execute:
```bash
docker run -p 8080:8080 rembab/search_engine:latest
```
...or run it through the docker desktop app.

### Method 2: run the project manually on your local environment
If you have **mix** and **Elixir** installed simply clone the repository and either **open the ui.livemd in livebook** or **use your preferred CLI together with mix** (this limits you to the command line interface):
```bash
iex -S mix
``` 

