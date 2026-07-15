# Contributing to RNArrow

Contributions are welcome. This includes bug reports, questions, and pull
requests.

## Reporting a bug

Open an issue at
[github.com/KaranSinghDev/RNTuple-Arrow-Gateway/issues](https://github.com/KaranSinghDev/RNTuple-Arrow-Gateway/issues).
Please include:

- What you expected to happen, and what happened instead
- Your ROOT and Arrow versions (run `root-config --version`)
- A small RNTuple file or a script that shows the problem, if you can

## Asking a question

Open an issue and add the `question` label. There is no mailing list or chat
channel.

## Requesting a feature

Open an issue and describe your use case. New type support (for example,
nested collections) and Flight features are the most likely additions.

## Sending a pull request

1. Fork the repository and create a branch from `main`.
2. Build the project and run the tests. All tests must pass.
3. Keep the code style consistent. The repository includes `.clang-format` and
   `.clang-tidy` files. Please run them before you submit.
4. Add a test for any change in behaviour. Correct Arrow output is the main
   guarantee of this project.
5. Open the pull request against `main`. Explain what you changed and why.

## Building and testing

With Docker (recommended):

```bash
docker build -f docker/dev.Dockerfile -t rag-env:dev .

docker run --rm -v "$(pwd)":/workspace -w /workspace rag-env:dev bash -lc "
  cmake --preset release &&
  cmake --build --preset release --parallel 2 &&
  ctest --test-dir build/release --output-on-failure
"
```

Without Docker, you need CMake 3.22 or newer, ROOT 6.36 or newer, Apache Arrow
C++ 19.0.0 with Flight, pybind11, and Python 3 with `pyarrow` and `pytest`:

```bash
cmake --preset release
cmake --build --preset release --parallel 2
ctest --test-dir build/release --output-on-failure
```

## Scope

RNArrow is a prototype. Its goal is to read RNTuple data and expose it as
Arrow. Changes that add type support, improve performance, or make the Flight
server more robust are in scope. Changes that add ROOT features unrelated to
Arrow output, or that tie the engine to one experiment's framework, are not.

## Code of conduct

If you take part in this project, you agree to follow the
[Code of Conduct](CODE_OF_CONDUCT.md).
