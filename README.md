# Breakdown of the assessment
This README serves as a detailed explanation of Aubrey Hughes' submission for the Data Engineer exercise for Brightwheel.

## Github structure
The structure of the repo has 2 folders. The `data` folder which contains the CSV files, and the `scripts` folder which contains the Python file for processing the CSV files.

## Installation/Running the service
You need to just clone into the repo (I prefer VS code) and right click on the `leads.py` file and click `Run Current File in Interactive Window` to see it run.

## Tradeoffs/Left out/Might do differently/Long Term ETL Strategies
Overall, I tried to show some techniques for standardizing the data but wasn't able to show everything. I included comments in the code to show how I would go about standardizing the data with more time.

# Ideal Infrastructure/Monitoring/Orchestration
The way that the Python file is set up, it currently reads in 3 CSV files from a directory located in the same repo, and also loads all of the data in to the database. This does not allow for a dynamic process and isn't how the ideal infrastructure would be set up. Part of the long term ETL strategy would be to have the Python file live inside of a Lambda, where the lambda is set on a weekly schedule (in CloudWatch) to process all CSV files in the S3 bucket that the 3rd party source loads them into. The Lambda could read in all of the files, standardize them, concatenate them into 1 dataframe, and then save to the Postgres database. An additional piece of infrastruce could be to create a different S3 bucket that houses all of the CSV files that have been processed for historical tracking (you can call this a "historical' bucket). Once the CSV files in the "active" bucket have been processed for the week, you could move them over to the "historical" bucket so the "active" bucket doesn't become flooded with files. Monitoring could be added to this by having an AWS SNS notify a pre-determined group of people whenever the lambda fails.
Also, since the new files will include a full refresh of existing and net new records, CDC needs to be done before data processing so that we only process and save new leads to the database.
Potential pitfall: Lambdas have a 15 minute timer, so if the amount of data per week becomes too much, then could potentially look to move data processing to an EMR cluster that submits a Python job.

## Scaling
Having a Lambda that can process all of the files within an S3 bucket could potentially not scale, which is why I mentioned above that a potential solution for scaling is to move the data processing from a Lambda to an EMR cluster for more processing power.

## Short term vs long term ETL strategy
Given that the file schemas are subject to change at any moment, and we want to make sure that the 3rd party user experience has schema flexibility, a short term solution that allows for changing schemas is that every week we save the standardized dataset to a new table in our Postgres database. This is not an ideal solution, but I believe it is necessary for the short term due to so many changing variables. Over time, as the schemas and process become more standardized, we can have one master `leads` table that includes a full refresh of existing and net new records to be saved in the database. Once the data is saved to the database, there could be additional transformations done to figure out old leads vs new leads.

# Resulting leads
Since resulting leads will need to be made available for outreach on a weekly basis, after the data is standardized and loaded into the database, there would need to be a way to communicate the leads out to necessary recipients. This could be done a multitude of ways, the easiest is probably having a dashboard that is connected to the database that can show resulting leads. You could also have the Lambda email out the new leads every week so people don't have to go check for it.
