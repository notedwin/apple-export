# Manzana Health

Manzana health is a module to process and export apple health into a tabular format in PostgreSQL or a duckdb file.

## Benefits

- There are many other apple health xml format processors, most of them don't allow any format schema, as people track different things such as calories using myfitness pal

## Installation

```bash
pip install manzana_health
```

## Prerequisties

Must have an Apple health archive

From my current knowledge there is no automated way to do this.
You must follow the instructions below:

- Open the Health app.
- Tap on your profile in the top right.
- Tap Export All Health Data.
- Share the archive with yourself (e.g. via AirDrop, Files, Mail, etc.).

## Usage

The default setup is for the archive being saved to ~/Downloads/
Otherwise, you need to pass the archive path to input_file variable.

```bash
python3 -m manzana_health --duckdb-file=./file.db --min-rows 100

python3 -m manzana_health --help

python3 -m manzana_health --health-file /Users/<user>/Downloads/export.zip --postgres-url 'postgresql://<user>:<password>@<server>/<db>' --min-rows 100
```

## Roadmap

- Add a screen recording of how to export health data.

## Resources

<https://dfir.pubpub.org/pub/xqvcn3hj/release/1>
<https://gist.github.com/hoffa/936db2bb85e134709cd263dd358ca309>
