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
