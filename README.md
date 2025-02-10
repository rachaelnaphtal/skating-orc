# Generating Judging Anomaly Reports

This project generates OAC style reports for judges.

More instructions to come, but full reports can be run through running download_results.py.

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
