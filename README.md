# Project-2 (Twitter Data Analysis)

## Project Description
Project 2 Analysis was performed on streamed Twitter data using the Spark Framework and SparkSql. Our intended purpose was to answer various questions regarding the conditions under which a user will choose to share their location information with the Twitter application. We utilized both static data that was previously streamed as well as information from other APIs. We also integrated Structured Streaming into our analyses of live tweets in our effort to analyze the conditions under which users will willingly share their location data with the twitter application and by extension the streaming Twitter API.

## Technology used
- Scala 2.12.12
- Spark 2.4.7
- hadoop 2.7.7
- Spark Sql 2.4.7

## Features
List of features ready - The analysis consist of answer of the following questions by analysing streaming twitter data.
- Does the time of day influence their decision to share their location?
- Are users more inclined to share their location when making positive tweets?
- Does the political philosophy of a region influence a users’ decision to share his/her location?
- Does gender influence a user’s decision to share their location?
- Is location sharing influenced by the device used to tweet?"
- How does the location listed in a user's profile match up with the locations provided by their tweets?

To-do list for future development:
- Other complex analysis can be done 
## Getting Started
Requirements / Tools
 - if you have windows then you need to install wls2 if you have linux/ubuntu wsl not required because wsl is to run linux in windows os. https://docs.microsoft.com/en-us/windows/wsl/install-win10
 - have a text editor like visual studio code.
 - install hadoop 2.7.7 https://hadoop.apache.org/docs/r2.7.7/hadoop-project-dist/hadoop-common/SingleCluster.html
 - git clone https://github.com/Frank-Rauen/Project-2 in linux terminal through wsl
 - For each question first run streaming code by just uncommention function call to streaming code.
 - Then compile the code by using sbt assebmly
 - Then run the code by using spark-submit target/scala-2.12/projecttwo-assembly-0.1.0-SNAPSHOT.jar 
 - After Streaming data. You uncomment analysis code. And again compile and run. 

## Usage
First run the code for streaming part. And then analysis part

## Contributors
Its team project, w ith team members Tyler Westbrook, Raju Rana, Frank Rauen

## License
This project uses the following license: Its open source. Any one can use it modify it as they want. and can built their own project.
