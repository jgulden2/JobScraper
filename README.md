"# JobScraper" 
"# JobScraper" 
"# JobScraper" 
python main.py --testing true --scrapers lockheedmartin --logfile run.log

python main.py --testing true --logfile run.log --combine-full 

python main.py --scrapers rtx --limit 15 --db-url sqlite:///.
/.cache/jobs.sqlite --db-table jobs --db-mode min --logfile run.log --no-db-skip-existing


To open frontend on CMD:
cd to jobs-ui
npm run dev

To open backend on CMD:
cd to site_by_site
python api/app.py