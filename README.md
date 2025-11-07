"# JobScraper" 
"# JobScraper" 
"# JobScraper" 
python main.py --testing true --scrapers lockheedmartin --logfile run.log

python main.py --testing true --logfile run.log --combine-full 

python main.py --scrapers rtx --limit 15 --db-url sqlite:///.
/.cache/jobs.sqlite --db-table jobs --db-mode min --logfile run.log --no-db-skip-existing