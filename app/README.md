## Running the Synthesis example

* [Download ollama](https://ollama.com/download/linux)
* Download llama3:
    * ollama pull llama3
* Set the port to 8080.
    * export OLLAMA_HOST=127.0.0.1:8080
* Check where you model was downloaded to and set that to the models path (the models environment variable pointed to nothing for me so I made it point to the shared model registry - the default ~/.ollama/models/ was empty):
    * export OLLAMA_MODELS=/usr/share/ollama/.ollama/models/
* Start an ollama server:
    * ollama serve

In a separate window:
* [Download and install GHC + cabal](https://www.haskell.org/ghcup/install/)
* From the top level directory (the `dataframe` directory) - run `cabal run synthesis`.

