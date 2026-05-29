# Generating Judging Anomaly Reports

This project generates OAC style reports for judges.

More instructions to come, but full reports can be run through running download_results.py.

## IJS discovery and batch load

To find USFS IJS competitions by numeric ID and load them from a CSV (discover → metadata → full scrape), see **[scripts/README.md](scripts/README.md)**.

## ISU and international figure skating results

To gather ISU API-linked figure skating **Detailed Results** URLs, including international competitions, use:

```bash
python scripts/load_isu_figure_skating_results.py --seasons 2526 --event-levels All -o figure_2526.csv
```

Common examples:

```bash
# ISU events only for season 2526
python scripts/load_isu_figure_skating_results.py --seasons 2526 -o isu_2526.csv

# International competitions only for season 2526
python scripts/load_isu_figure_skating_results.py --seasons 2526 --event-levels International -o international_2526.csv

# ISU + international competitions by calendar year
python scripts/load_isu_figure_skating_results.py --year 2025 --event-levels All -o figure_2025.csv
```

Compact season codes like `2526` are expanded to ISU API seasons like `2025/2026`. Add `--load --skip-if-in-database` to load discovered result pages into the database after writing the CSV. Full script details are in **[scripts/README.md](scripts/README.md)**.

To write the season `2526` ISU + international competitions directly to the database:

```bash
python scripts/load_isu_figure_skating_results.py \
  --seasons 2526 \
  --event-levels All \
  --load \
  --skip-if-in-database \
  --quiet
```

Database load behavior:

- `--event-levels All` loads both API categories: `ISU` and `International`. Use `--event-levels ISU` or `--event-levels International` for only one category.
- `competition.name` uses the ISU API event name exactly, for example `Lake Placid International Ice Dance Competition 2025`.
- `competition.year` uses the compact season code, for example `2526`.
- `competition.results_url` uses the normalized Detailed Results URL with `/index.htm` or `/index.asp` stripped.
- `start_date`, `end_date`, and `location` come from the ISU API event metadata.
- `competition.international` is set to `true` for every row loaded by this script.
- `officials_analysis.competition_type` is inferred automatically: `International` events use type `17`; ISU Championship-tier events (Worlds, World Junior, Europeans, Four Continents, World Synchro, Olympic Games) use type `15`; all other ISU events use type `16`.
- Use `--write-failures` or `--failures-output PATH` to save failed competitions to CSV.
- Use `--disciplines All` for figure + synchronized skating (default is figure only).
- `qualifying` and `nqs` are set to `false` for these international type IDs.
- Without `--metadata-only`, `--load` runs the full segment scrape through `downloadResults.scrape()`. Add `--metadata-only` to only create/update competition rows without scraping segments.

# Setup libraries
install homebrew or anaconda
install python
You may need to add homebrew python to your path which would be something like the folowing but with whichever python version you downloaded:
echo 'export PATH=/opt/homebrew/opt/python@3.13/libexec/bin:$PATH' >> ~/.zprofile
source ~/.zprofile

if issues with chromium driver and on new Macbooks you may need to install rosetta2 (softwareupdate --install-rosetta)

# Start up virtual development environment and download requirements
python3 -m venv ./venv
source venv/bin/activate
pip install -r requirements.txt
