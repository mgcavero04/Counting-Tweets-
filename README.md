# Counting-Tweets-
From two json tables, we want to we want to count the number of purchase made by each people. We will implement this using PySpark and a Spark Dataframe.

Explanation of the source code:
The source code begins importing the pyspark sql libraries.
In the main function of the program first, the spark session is created and open.
Then we begin the process uploading the json files in variables, we then read the files content into spark data frames, in this case I defined two data frames for each of the files: tweets_df
and cityStateMap_df. After that I proceed to show all the content of each data frame with functions: tweets_df.show and cityStateMap_df.show


As a second process I have built the transformation functions for each of the data frames:

•	To count the number of tweets published in each state using the ‘cityStateMap_df’ and 
Since I want to show the state (cityStateMap_df) and sum of cities tweets(tweets_df)
I have built a join query between these two dataframes where their key intersection is the geo (tweets_df) and city (cityStateMap_df ) columns respectively. 
To be able to show each of those states(cityStateMap_df ) we need to group by that column(state), and then make a count of the join, to show the total sum if that grouping.
In this way the third process is the action function : count().
