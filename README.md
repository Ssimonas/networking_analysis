# Networking analysis (2022)

Notebook analyses (aggragates data) and provides suggestion to which servers are not performing well and could be switched off.

### Notebook networking.ipynb
Main file to use: <b>networking.ipynb</b><br />
<b>It is required to run 1st cell of notebook</b> to install all required packages from requirements.txt<br />
<b>MOST</b> cells of the notebook need to be executed as most of them provide/alter the data which is used to take the final decision on classification.<br />


### The result
The results (which servers could be switched off) are stored in <b>bad_servers.csv</b> file. <br/>


### Data and database
Data is stored locally in PostgreSQL database, to which a connection is made.<br />
<b>Constants.py</b> file is not pushed to git as it contains password to the database. <br />
The data format of the file is a single row:<br />
DB_PASSWORD = 'some_password_to_the_database'<br />


### Comments and insights
Most of comments and insights are provided in Notebook in appropriate places. <br />
Comments for specific functions and functions themselves can be found in files:
* <b>..functions/agg_data_with_stats.py</b>
* <b>..functions/clustering_and_classifying.py</b>
